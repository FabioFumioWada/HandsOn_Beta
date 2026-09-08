"""Ingestão Bronze isolada da ANEEL.

O script consome arquivos previamente baixados pelo downloader compartilhado
(`02_baixar_arquivos_portais.py --fontes ANEEL`) e processa somente arquivos
ANEEL em CSV, XLS ou XLSX. PDFs de dicionário publicados pelo portal são
mantidos no inventário, mas classificados como inelegíveis para ingestão
 tabular.

O fluxo cria inventário, seleciona formatos canônicos, grava tabelas Delta por
conjunto de dados, gera o dicionário de campos e registra cada etapa no
controle global `handson_beta.controle_global.controle_importacao`.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from functools import reduce
from pathlib import PurePosixPath
from typing import Any, DefaultDict, Dict, List, Mapping, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


LANDING_ZONE = "/Volumes/handson_beta/landing_zone/arquivos/"
BRONZE_CATALOG = "handson_beta"
SOURCE_PREFIX = "ANEEL"
SOURCE_SCHEMA = "bronze_aneel"
BRONZE_SCHEMA = f"{BRONZE_CATALOG}.{SOURCE_SCHEMA}"
GLOBAL_CONTROL_SCHEMA = f"{BRONZE_CATALOG}.controle_global"
GLOBAL_CONTROL_TABLE = f"{GLOBAL_CONTROL_SCHEMA}.controle_importacao"
FILE_LIST_TABLE = f"{BRONZE_SCHEMA}.lista_arquivos_aneel"
DICTIONARY_TABLE = f"{BRONZE_SCHEMA}.dicionario_dados_aneel"
ELIGIBLE_EXTENSIONS = {".csv", ".xls", ".xlsx"}
FORMAT_PRIORITY = {".csv": 0, ".xlsx": 1, ".xls": 2}
YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
RUN_TS = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .strip()
    )


def sanitize_identifier(value: Any, fallback: str = "dataset_sem_nome") -> str:
    text = normalize_text(value)
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    if not text:
        return fallback
    if text[0].isdigit():
        text = f"c_{text}"
    return text[:250]


def source_year(file_name: str) -> Optional[int]:
    match = YEAR_PATTERN.search(file_name)
    return int(match.group(0)) if match else None


def dataset_key(file_name: str) -> str:
    stem = PurePosixPath(file_name).stem
    stem = re.sub(r"^ANEEL[-_ ]+", "", stem, flags=re.IGNORECASE)
    stem_without_year = YEAR_PATTERN.sub("", stem)
    stem_without_year = re.sub(r"[_\- ]{2,}", "_", stem_without_year).strip("_ -")
    return f"aneel_{sanitize_identifier(stem_without_year)}"


def unique_column_names(columns: Sequence[str]) -> List[str]:
    used: Dict[str, int] = defaultdict(int)
    output: List[str] = []
    reserved = {
        "ano",
        "arquivo_origem",
        "caminho_origem",
        "aba_origem",
        "data_modificacao_origem",
        "chave_duplicidade",
        "indice_duplicidade",
        "quantidade_duplicidade",
        "registro_duplicado",
        "data_ingestao_utc",
    }
    for raw_name in columns:
        name = sanitize_identifier(raw_name, fallback="campo")
        if name in reserved:
            name = f"orig_{name}"
        used[name] += 1
        output.append(name if used[name] == 1 else f"{name}_{used[name]}")
    return output


def python_read_path(path: str) -> str:
    if path.startswith("dbfs:/Volumes/"):
        return "/Volumes/" + path[len("dbfs:/Volumes/") :]
    return path


def list_source_files(path: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    pending = [path.rstrip("/")]
    prefix_normalized = SOURCE_PREFIX.casefold()
    while pending:
        current = pending.pop()
        for info in dbutils.fs.ls(current):  # noqa: F821 - disponível no Databricks
            item_path = info.path
            if item_path.endswith("/"):
                pending.append(item_path.rstrip("/"))
                continue
            file_name = PurePosixPath(item_path).name
            if not file_name.casefold().startswith(prefix_normalized):
                continue
            extension = PurePosixPath(file_name).suffix.lower()
            eligible_extension = extension in ELIGIBLE_EXTENSIONS
            entries.append(
                {
                    "arquivo_origem": file_name,
                    "caminho_origem": item_path,
                    "extensao": extension or None,
                    "formato": extension.lstrip(".").upper() if extension else None,
                    "elegivel_importacao": eligible_extension,
                    "elegivel_extensao": eligible_extension,
                    "nome_dataset": dataset_key(file_name),
                    "ano": source_year(file_name),
                    "tamanho_bytes": int(getattr(info, "size", 0) or 0),
                    "data_modificacao_ms": int(getattr(info, "modificationTime", 0) or 0),
                }
            )
    return sorted(entries, key=lambda item: item["caminho_origem"].lower())


def select_canonical_files(
    source_files: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    eligible = [item for item in source_files if item["elegivel_importacao"]]
    grouped: DefaultDict[Tuple[str, Optional[int]], List[Dict[str, Any]]] = defaultdict(list)
    for item in eligible:
        grouped[(item["nome_dataset"], item["ano"])].append(item)

    selected: List[Dict[str, Any]] = []
    alternatives: List[Dict[str, Any]] = []
    for key in sorted(grouped, key=lambda value: (value[0], value[1] or 0)):
        group = grouped[key]
        chosen = sorted(
            group,
            key=lambda item: (
                FORMAT_PRIORITY.get(item["extensao"], 99),
                item["caminho_origem"].lower(),
            ),
        )[0]
        selected.append(chosen)
        alternatives.extend(
            item for item in group if item["caminho_origem"] != chosen["caminho_origem"]
        )
    return selected, alternatives


def infer_csv_delimiter(path: str) -> str:
    try:
        sample = dbutils.fs.head(path, 65536)  # noqa: F821 - disponível no Databricks
    except Exception:
        return ";"
    lines = [line for line in sample.splitlines() if line.strip()]
    if not lines:
        return ";"
    header = lines[0]
    candidates = {delimiter: header.count(delimiter) for delimiter in [";", ",", "\t", "|"]}
    delimiter, count = max(candidates.items(), key=lambda item: item[1])
    return delimiter if count > 0 else ";"


def read_csv(path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .option("emptyValue", "")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("sep", infer_csv_delimiter(path))
        .option("mode", "PERMISSIVE")
        .load(path)
    )


def as_string_columns(df: DataFrame) -> DataFrame:
    return df.select(*[F.col(column).cast("string").alias(column) for column in df.columns])


def read_excel(path: str) -> List[Tuple[str, DataFrame]]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError(
            "A leitura ANEEL XLS/XLSX requer pandas no ambiente efetivo do Job."
        ) from exc

    extension = PurePosixPath(path).suffix.lower()
    engine = "xlrd" if extension == ".xls" else "openpyxl"
    if importlib.util.find_spec(engine) is None:
        raise RuntimeError(
            f"A leitura {extension} requer o pacote {engine} no Python {sys.executable}."
        )

    workbook = pd.ExcelFile(python_read_path(path), engine=engine)
    frames: List[Tuple[str, DataFrame]] = []
    for sheet_name in workbook.sheet_names:
        pdf = pd.read_excel(workbook, sheet_name=sheet_name, dtype=object)
        pdf = pdf.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if pdf.empty:
            continue
        pdf.columns = unique_column_names([str(column) for column in pdf.columns])
        schema = T.StructType(
            [T.StructField(str(column), T.StringType(), True) for column in pdf.columns]
        )
        rows = [
            tuple(None if pd.isna(value) else str(value) for value in row)
            for row in pdf.itertuples(index=False, name=None)
        ]
        frames.append((sheet_name, spark.createDataFrame(rows, schema=schema)))
    return frames


def add_source_metadata(
    df: DataFrame, file_info: Dict[str, Any], sheet_name: Optional[str]
) -> DataFrame:
    renamed = df.toDF(*unique_column_names(df.columns))
    return (
        renamed.withColumn("ano", F.lit(file_info["ano"]).cast("int"))
        .withColumn("arquivo_origem", F.lit(file_info["arquivo_origem"]))
        .withColumn("caminho_origem", F.lit(file_info["caminho_origem"]))
        .withColumn("aba_origem", F.lit(sheet_name))
        .withColumn(
            "data_modificacao_origem",
            F.lit(file_info["data_modificacao_ms"]).cast("long"),
        )
        .withColumn("data_ingestao_utc", F.lit(RUN_TS))
    )


def add_duplicate_indices(df: DataFrame) -> DataFrame:
    source_columns = [
        column
        for column in df.columns
        if column
        not in {
            "arquivo_origem",
            "caminho_origem",
            "aba_origem",
            "data_modificacao_origem",
            "data_ingestao_utc",
        }
    ]
    if not source_columns:
        source_columns = ["arquivo_origem"]
    hash_parts = [F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in source_columns]
    result = df.withColumn("chave_duplicidade", F.sha2(F.concat_ws("||", *hash_parts), 256))
    duplicate_window = Window.partitionBy("chave_duplicidade").orderBy(
        F.col("ano").asc_nulls_last(),
        F.col("arquivo_origem").asc(),
        F.col("caminho_origem").asc(),
        F.col("aba_origem").asc_nulls_last(),
    )
    count_window = Window.partitionBy("chave_duplicidade")
    return (
        result.withColumn("quantidade_duplicidade", F.count(F.lit(1)).over(count_window))
        .withColumn("indice_duplicidade", F.row_number().over(duplicate_window))
        .withColumn("registro_duplicado", F.col("quantidade_duplicidade") > 1)
    )


def write_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def control_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("fonte", T.StringType(), False),
            T.StructField("etapa", T.StringType(), False),
            T.StructField("arquivo_origem", T.StringType(), True),
            T.StructField("caminho_origem", T.StringType(), True),
            T.StructField("nome_dataset", T.StringType(), True),
            T.StructField("tabela_destino", T.StringType(), True),
            T.StructField("status", T.StringType(), False),
            T.StructField("quantidade_registros", T.LongType(), True),
            T.StructField("mensagem", T.StringType(), True),
            T.StructField("data_execucao_utc", T.StringType(), False),
        ]
    )


def append_control(rows: List[Dict[str, Any]]) -> None:
    if rows:
        (
            spark.createDataFrame(rows, schema=control_schema())
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(GLOBAL_CONTROL_TABLE)
        )


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GLOBAL_CONTROL_SCHEMA}")

source_files = list_source_files(LANDING_ZONE)
if not source_files:
    raise RuntimeError(
        f"Nenhum arquivo iniciado por {SOURCE_PREFIX} foi encontrado em {LANDING_ZONE}. "
        "Execute primeiro o downloader ANEEL."
    )

selected_files, alternative_files = select_canonical_files(source_files)
selected_format_counts: Dict[str, int] = defaultdict(int)
for selected_file in selected_files:
    selected_format_counts[selected_file["extensao"]] += 1
print(f"Formatos canônicos ANEEL selecionados: {dict(sorted(selected_format_counts.items()))}")

control_rows: List[Dict[str, Any]] = []
file_list_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("fonte", T.StringType(), False),
        T.StructField("arquivo_origem", T.StringType(), False),
        T.StructField("caminho_origem", T.StringType(), False),
        T.StructField("extensao", T.StringType(), True),
        T.StructField("formato", T.StringType(), True),
        T.StructField("elegivel_importacao", T.BooleanType(), False),
        T.StructField("elegivel_extensao", T.BooleanType(), False),
        T.StructField("selecionado_ingestao", T.BooleanType(), False),
        T.StructField("nome_dataset", T.StringType(), False),
        T.StructField("ano", T.IntegerType(), True),
        T.StructField("tamanho_bytes", T.LongType(), True),
        T.StructField("data_modificacao_ms", T.LongType(), True),
        T.StructField("data_listagem_utc", T.StringType(), False),
    ]
)

selected_paths = {item["caminho_origem"] for item in selected_files}
file_list_rows = [
    {
        **item,
        "run_id": RUN_ID,
        "fonte": SOURCE_PREFIX,
        "selecionado_ingestao": item["caminho_origem"] in selected_paths,
        "data_listagem_utc": RUN_TS,
    }
    for item in source_files
]
write_table(spark.createDataFrame(file_list_rows, schema=file_list_schema), FILE_LIST_TABLE)
control_rows.append(
    {
        "run_id": RUN_ID,
        "fonte": SOURCE_PREFIX,
        "etapa": "LISTAGEM_LANDING_ZONE",
        "arquivo_origem": None,
        "caminho_origem": LANDING_ZONE,
        "nome_dataset": None,
        "tabela_destino": FILE_LIST_TABLE,
        "status": "SUCESSO",
        "quantidade_registros": len(source_files),
        "mensagem": (
            f"Foram listados {len(source_files)} arquivos ANEEL; "
            f"{len(selected_files)} foram selecionados após o filtro de extensão."
        ),
        "data_execucao_utc": RUN_TS,
    }
)

for file_info in source_files:
    if file_info["elegivel_extensao"]:
        continue
    control_rows.append(
        {
            "run_id": RUN_ID,
            "fonte": SOURCE_PREFIX,
            "etapa": "VALIDACAO_ARQUIVO",
            "arquivo_origem": file_info["arquivo_origem"],
            "caminho_origem": file_info["caminho_origem"],
            "nome_dataset": file_info["nome_dataset"],
            "tabela_destino": None,
            "status": "NAO_ELEGIVEL_EXTENSAO",
            "quantidade_registros": None,
            "mensagem": "Arquivo ANEEL fora do escopo CSV/XLS/XLSX; mantido somente no inventário.",
            "data_execucao_utc": RUN_TS,
        }
    )

for file_info in alternative_files:
    control_rows.append(
        {
            "run_id": RUN_ID,
            "fonte": SOURCE_PREFIX,
            "etapa": "VALIDACAO_ARQUIVO",
            "arquivo_origem": file_info["arquivo_origem"],
            "caminho_origem": file_info["caminho_origem"],
            "nome_dataset": file_info["nome_dataset"],
            "tabela_destino": None,
            "status": "NAO_SELECIONADO_FORMATO_CANONICO",
            "quantidade_registros": None,
            "mensagem": (
                "Recurso alternativo do mesmo dataset/ano; o formato canônico foi "
                "selecionado para evitar duplicidade."
            ),
            "data_execucao_utc": RUN_TS,
        }
    )

files_by_dataset: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
for file_info in selected_files:
    files_by_dataset[file_info["nome_dataset"]].append(file_info)

dictionary_rows: List[Dict[str, Any]] = []
ingestion_errors = 0
created_tables: List[str] = []

for dataset_name, group_files in sorted(files_by_dataset.items()):
    table_name = f"{BRONZE_SCHEMA}.{sanitize_identifier(dataset_name)}"
    source_names = [item["arquivo_origem"] for item in group_files]
    try:
        frames: List[DataFrame] = []
        for file_info in group_files:
            extension = file_info["extensao"]
            if extension == ".csv":
                raw_df = read_csv(file_info["caminho_origem"])
                frames.append(add_source_metadata(as_string_columns(raw_df), file_info, None))
            else:
                for sheet_name, sheet_df in read_excel(file_info["caminho_origem"]):
                    frames.append(
                        add_source_metadata(
                            as_string_columns(sheet_df), file_info, sheet_name
                        )
                    )

        if not frames:
            raise RuntimeError("Nenhum DataFrame foi produzido para o dataset ANEEL.")
        combined = reduce(
            lambda left, right: left.unionByName(right, allowMissingColumns=True),
            frames,
        )
        bronze_df = add_duplicate_indices(combined)
        write_table(bronze_df, table_name)
        row_count = bronze_df.count()
        created_tables.append(table_name)

        for field in bronze_df.schema.fields:
            dictionary_rows.append(
                {
                    "fonte": SOURCE_PREFIX,
                    "schema_bronze": SOURCE_SCHEMA,
                    "nome_tabela": table_name,
                    "nome_dataset": dataset_name,
                    "campo": field.name,
                    "tipo_spark": field.dataType.simpleString(),
                    "nullable": bool(field.nullable),
                    "finalidade_descricao": f"Campo carregado do conjunto ANEEL {dataset_name}.",
                    "origens": ", ".join(source_names),
                    "data_atualizacao_utc": RUN_TS,
                }
            )

        control_rows.append(
            {
                "run_id": RUN_ID,
                "fonte": SOURCE_PREFIX,
                "etapa": "INGESTAO_BRONZE",
                "arquivo_origem": ", ".join(source_names),
                "caminho_origem": ", ".join(item["caminho_origem"] for item in group_files),
                "nome_dataset": dataset_name,
                "tabela_destino": table_name,
                "status": "SUCESSO",
                "quantidade_registros": row_count,
                "mensagem": "Dataset ANEEL importado com normalização segura de tipos.",
                "data_execucao_utc": RUN_TS,
            }
        )
    except Exception as exc:
        ingestion_errors += 1
        control_rows.append(
            {
                "run_id": RUN_ID,
                "fonte": SOURCE_PREFIX,
                "etapa": "INGESTAO_BRONZE",
                "arquivo_origem": ", ".join(source_names),
                "caminho_origem": ", ".join(item["caminho_origem"] for item in group_files),
                "nome_dataset": dataset_name,
                "tabela_destino": table_name,
                "status": "ERRO",
                "quantidade_registros": None,
                "mensagem": repr(exc)[:4000],
                "data_execucao_utc": RUN_TS,
            }
        )

if dictionary_rows:
    dictionary_schema = T.StructType(
        [
            T.StructField("fonte", T.StringType(), False),
            T.StructField("schema_bronze", T.StringType(), False),
            T.StructField("nome_tabela", T.StringType(), False),
            T.StructField("nome_dataset", T.StringType(), False),
            T.StructField("campo", T.StringType(), False),
            T.StructField("tipo_spark", T.StringType(), False),
            T.StructField("nullable", T.BooleanType(), True),
            T.StructField("finalidade_descricao", T.StringType(), True),
            T.StructField("origens", T.StringType(), True),
            T.StructField("data_atualizacao_utc", T.StringType(), False),
        ]
    )
    write_table(
        spark.createDataFrame(dictionary_rows, schema=dictionary_schema),
        DICTIONARY_TABLE,
    )

append_control(control_rows)

print(f"Fonte processada: {SOURCE_PREFIX}")
print(f"Arquivos ANEEL listados: {len(source_files)}")
print(f"Arquivos ANEEL elegíveis por extensão: {sum(1 for item in source_files if item['elegivel_extensao'])}")
print(f"Arquivos ANEEL selecionados: {len(selected_files)}")
print(f"Datasets ANEEL processados: {len(files_by_dataset)}")
print(f"Tabelas Bronze ANEEL criadas: {len(created_tables)}")
print(f"Erros de ingestão: {ingestion_errors}")
print(f"Tabela de listagem: {FILE_LIST_TABLE}")
print(f"Tabela de dicionário: {DICTIONARY_TABLE}")
print(f"Schema de dados: {BRONZE_SCHEMA}")
print(f"Controle global: {GLOBAL_CONTROL_TABLE}")

if ingestion_errors:
    raise RuntimeError(
        f"A ingestão ANEEL terminou com {ingestion_errors} dataset(s) em ERRO. "
        f"Consulte {GLOBAL_CONTROL_TABLE} antes de promover o Job."
    )

spark.table(GLOBAL_CONTROL_TABLE).filter(F.col("run_id") == RUN_ID).orderBy("etapa", "status").show(truncate=False)
