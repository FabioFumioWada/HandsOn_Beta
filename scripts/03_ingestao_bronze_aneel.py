"""Ingestão Bronze específica da ANEEL.

Fluxo executado no Databricks como Python file:

1. lista todos os arquivos cujo nome inicia com ANEEL na Landing Zone;
2. grava a listagem em handson_beta.bronze_aneel.lista_arquivos_aneel;
3. cria o schema handson_beta.bronze_aneel;
4. importa somente arquivos ANEEL CSV/XLS/XLSX, agrupando por nome sem o ano;
5. cria tabelas Delta gerenciadas, uma por conjunto de dados;
6. cria o dicionário de dados da fonte ANEEL; e
7. grava controle e log em handson_beta.controle_global.controle_importacao.

Para reutilizar o fluxo em outra fonte, altere SOURCE_PREFIX e SOURCE_SCHEMA.
A tabela global de controle não deve ser recriada com outro nome.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from functools import reduce
from pathlib import PurePosixPath
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------------------------------------------------------
# Parâmetros da fonte
# -----------------------------------------------------------------------------
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
YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
RUN_TS = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


# -----------------------------------------------------------------------------
# Utilitários
# -----------------------------------------------------------------------------
def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower().strip()


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
    stem_without_year = YEAR_PATTERN.sub("", stem)
    stem_without_year = re.sub(r"[_\- ]{2,}", "_", stem_without_year).strip("_ -")
    return sanitize_identifier(stem_without_year)


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


def list_source_files(path: str) -> List[Dict[str, Any]]:
    """Lista todos os arquivos ANEEL, incluindo extensões não elegíveis."""
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
            extension = PurePosixPath(item_path).suffix.lower()
            entries.append(
                {
                    "arquivo_origem": file_name,
                    "caminho_origem": item_path,
                    "extensao": extension or None,
                    "elegivel_importacao": extension in ELIGIBLE_EXTENSIONS,
                    "nome_dataset": dataset_key(file_name),
                    "ano": source_year(file_name),
                    "tamanho_bytes": int(getattr(info, "size", 0) or 0),
                    "data_modificacao_ms": int(getattr(info, "modificationTime", 0) or 0),
                }
            )
    return sorted(entries, key=lambda item: item["caminho_origem"].lower())


def infer_csv_delimiter(path: str) -> str:
    try:
        sample = dbutils.fs.head(path, 65536)  # noqa: F821 - disponível no Databricks
    except Exception:
        return ","
    lines = [line for line in sample.splitlines() if line.strip()]
    if not lines:
        return ","
    header = lines[0]
    candidates = {delimiter: header.count(delimiter) for delimiter in [",", ";", "\t", "|"]}
    delimiter, count = max(candidates.items(), key=lambda item: item[1])
    return delimiter if count > 0 else ","


def read_csv(path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("sep", infer_csv_delimiter(path))
        .option("mode", "PERMISSIVE")
        .load(path)
    )


def read_excel(path: str) -> List[Tuple[str, DataFrame]]:
    """Lê todas as abas de arquivos XLS ou XLSX com a dependência correta."""
    try:
        import importlib.util
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError(
            "A leitura XLS/XLSX requer pandas no cluster Databricks. "
            "Instale pandas como biblioteca do cluster ou dependência do Job."
        ) from exc

    extension = PurePosixPath(path).suffix.lower()
    if extension not in {".xls", ".xlsx"}:
        raise ValueError(f"Extensão Excel não suportada pelo leitor: {extension}")
    engine = "xlrd" if extension == ".xls" else "openpyxl"
    package_command = "xlrd>=2.0.1" if engine == "xlrd" else "openpyxl>=3.1.0"
    if importlib.util.find_spec(engine) is None:
        raise RuntimeError(
            f"A leitura {extension} requer o pacote {engine}, mas ele não está disponível no Python efetivo. "
            f"Python: {sys.executable}. Origem esperada: biblioteca PyPI do Compute/Job. "
            f"Adicione `{package_command}` como biblioteca do Compute/Job, reinicie o Compute, abra uma nova sessão "
            f"e execute scripts/00_verificar_dependencias_excel.py antes da ingestão."
        )
    try:
        workbook = pd.ExcelFile(path, engine=engine)
    except ImportError as exc:
        raise RuntimeError(
            f"A leitura {extension} não conseguiu inicializar o engine {engine} no Python {sys.executable}. "
            f"Confirme a instalação PyPI `{package_command}` como biblioteca do Compute/Job, reinicie o Compute "
            f"e abra uma nova sessão antes da execução."
        ) from exc
    frames: List[Tuple[str, DataFrame]] = []
    for sheet_name in workbook.sheet_names:
        pdf = pd.read_excel(workbook, sheet_name=sheet_name, dtype=object)
        pdf = pdf.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if pdf.empty:
            continue
        pdf.columns = unique_column_names([str(column) for column in pdf.columns])
        frames.append((sheet_name, spark.createDataFrame(pdf)))
    return frames


def add_source_metadata(df: DataFrame, file_info: Dict[str, Any], sheet_name: Optional[str]) -> DataFrame:
    renamed = df.toDF(*unique_column_names(df.columns))
    return (
        renamed.withColumn("ano", F.lit(file_info["ano"]).cast("int"))
        .withColumn("arquivo_origem", F.lit(file_info["arquivo_origem"]))
        .withColumn("caminho_origem", F.lit(file_info["caminho_origem"]))
        .withColumn("aba_origem", F.lit(sheet_name))
        .withColumn("data_modificacao_origem", F.lit(file_info["data_modificacao_ms"]).cast("long"))
        .withColumn("data_ingestao_utc", F.lit(RUN_TS))
    )


def add_duplicate_indices(df: DataFrame) -> DataFrame:
    source_columns = [
        column
        for column in df.columns
        if column not in {"arquivo_origem", "caminho_origem", "aba_origem", "data_modificacao_origem", "data_ingestao_utc"}
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
    """Grava tabela Delta gerenciada, sem path de Volume."""
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
    if not rows:
        return
    control_df = spark.createDataFrame(rows, schema=control_schema())
    (
        control_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(GLOBAL_CONTROL_TABLE)
    )


# -----------------------------------------------------------------------------
# Execução
# -----------------------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GLOBAL_CONTROL_SCHEMA}")

source_files = list_source_files(LANDING_ZONE)
control_rows: List[Dict[str, Any]] = []

# Item 1: listagem de todos os arquivos iniciados por ANEEL.
file_list_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("fonte", T.StringType(), False),
        T.StructField("arquivo_origem", T.StringType(), False),
        T.StructField("caminho_origem", T.StringType(), False),
        T.StructField("extensao", T.StringType(), True),
        T.StructField("elegivel_importacao", T.BooleanType(), False),
        T.StructField("nome_dataset", T.StringType(), False),
        T.StructField("ano", T.IntegerType(), True),
        T.StructField("tamanho_bytes", T.LongType(), True),
        T.StructField("data_modificacao_ms", T.LongType(), True),
        T.StructField("data_listagem_utc", T.StringType(), False),
    ]
)
file_list_rows = [
    {
        **item,
        "run_id": RUN_ID,
        "fonte": SOURCE_PREFIX,
        "data_listagem_utc": RUN_TS,
    }
    for item in source_files
]
file_list_df = spark.createDataFrame(file_list_rows, schema=file_list_schema)
write_table(file_list_df, FILE_LIST_TABLE)

append_control(
    [
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
            "mensagem": f"Foram listados {len(source_files)} arquivos iniciados por {SOURCE_PREFIX}.",
            "data_execucao_utc": RUN_TS,
        }
    ]
)

# Item 2: importar somente CSV/XLSX iniciados por ANEEL.
eligible_files = [item for item in source_files if item["elegivel_importacao"]]
files_by_dataset: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
for file_info in eligible_files:
    files_by_dataset[file_info["nome_dataset"]].append(file_info)

# Registra arquivos ANEEL fora do escopo no controle global.
for file_info in source_files:
    if not file_info["elegivel_importacao"]:
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
                "mensagem": "Arquivo iniciado por ANEEL, mas fora do escopo CSV/XLS/XLSX.",
                "data_execucao_utc": RUN_TS,
            }
        )

# Item 2: cada conjunto ANEEL vira uma tabela no schema bronze_aneel.
dictionary_rows: List[Dict[str, Any]] = []
for dataset_key, group_files in sorted(files_by_dataset.items()):
    table_name = f"{BRONZE_SCHEMA}.{sanitize_identifier(dataset_key)}"
    source_names = [item["arquivo_origem"] for item in group_files]
    try:
        frames: List[DataFrame] = []
        for file_info in group_files:
            if file_info["extensao"] == ".csv":
                frames.append(add_source_metadata(read_csv(file_info["caminho_origem"]), file_info, None))
            else:
                for sheet_name, sheet_df in read_excel(file_info["caminho_origem"]):
                    frames.append(add_source_metadata(sheet_df, file_info, sheet_name))

        if not frames:
            raise RuntimeError("Nenhum DataFrame foi produzido para o conjunto.")
        combined = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)
        bronze_df = add_duplicate_indices(combined)
        write_table(bronze_df, table_name)
        row_count = bronze_df.count()

        for field in bronze_df.schema.fields:
            dictionary_rows.append(
                {
                    "fonte": SOURCE_PREFIX,
                    "schema_bronze": SOURCE_SCHEMA,
                    "nome_tabela": table_name,
                    "nome_dataset": dataset_key,
                    "campo": field.name,
                    "tipo_spark": field.dataType.simpleString(),
                    "nullable": bool(field.nullable),
                    "finalidade_descricao": f"Campo carregado do conjunto ANEEL {dataset_key}.",
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
                "nome_dataset": dataset_key,
                "tabela_destino": table_name,
                "status": "SUCESSO",
                "quantidade_registros": row_count,
                "mensagem": "Conjunto importado com sucesso.",
                "data_execucao_utc": RUN_TS,
            }
        )
    except Exception as exc:
        control_rows.append(
            {
                "run_id": RUN_ID,
                "fonte": SOURCE_PREFIX,
                "etapa": "INGESTAO_BRONZE",
                "arquivo_origem": ", ".join(source_names),
                "caminho_origem": ", ".join(item["caminho_origem"] for item in group_files),
                "nome_dataset": dataset_key,
                "tabela_destino": table_name,
                "status": "ERRO",
                "quantidade_registros": None,
                "mensagem": repr(exc)[:4000],
                "data_execucao_utc": RUN_TS,
            }
        )

# Dicionário específico da ANEEL.
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
    write_table(spark.createDataFrame(dictionary_rows, schema=dictionary_schema), DICTIONARY_TABLE)

# Item 3: grava o controle e o log global para reutilização pelas demais fontes.
append_control(control_rows)

print(f"Fonte processada: {SOURCE_PREFIX}")
print(f"Arquivos ANEEL listados: {len(source_files)}")
print(f"Arquivos ANEEL elegíveis importados: {len(eligible_files)}")
print(f"Tabela de listagem: {FILE_LIST_TABLE}")
print(f"Schema de dados: {BRONZE_SCHEMA}")
print(f"Controle global: {GLOBAL_CONTROL_TABLE}")

spark.table(GLOBAL_CONTROL_TABLE).filter(F.col("run_id") == RUN_ID).orderBy("etapa", "status").show(truncate=False)
