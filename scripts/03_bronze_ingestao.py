# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingestão da Landing Zone para a camada Bronze
# MAGIC
# MAGIC Este script é idempotente: a cada execução ele reprocessa somente os arquivos
# MAGIC elegíveis da Landing Zone, agrupa nomes ignorando o ano, grava tabelas Delta
# MAGIC na camada Bronze e atualiza o dicionário de dados consolidado.
<<<<<<< Updated upstream

from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from functools import reduce
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------------------------------------------------------
# Parâmetros do processo
# -----------------------------------------------------------------------------
LANDING_ZONE = "/Volumes/handson_beta/landing_zone/arquivos/"
BRONZE_ROOT = "/Volumes/handson_beta/bronze/"
BRONZE_SCHEMA = "handson_beta.bronze"
DICTIONARY_TABLE = f"{BRONZE_SCHEMA}.dicionario_dados_bronze"
AUDIT_TABLE = f"{BRONZE_SCHEMA}.controle_carga_bronze"
RUN_TS = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

ELIGIBLE_EXTENSIONS = {".csv", ".xlsx"}
YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
DICTIONARY_NAME_PATTERN = re.compile(
    r"(dicionario|dicionário|dictionary|metadado|metadata|layout|legenda|descricao|descrição)",
    flags=re.IGNORECASE,
)
RESERVED_COLUMNS = {
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

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


# -----------------------------------------------------------------------------
# Utilitários de nomes, arquivos e esquemas
# -----------------------------------------------------------------------------
def normalize_text(value: Any) -> str:
    """Normaliza texto para comparação de nomes de tabelas e campos."""
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return text.lower().strip()


def sanitize_identifier(value: Any, fallback: str = "campo") -> str:
    """Converte nomes de origem em identificadores Spark/SQL estáveis."""
    text = normalize_text(value)
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    if not text:
        text = fallback
    if text[0].isdigit():
        text = f"c_{text}"
    return text[:250]


def unique_column_names(columns: Sequence[str]) -> List[str]:
    """Garante nomes únicos, preservando a ordem das colunas de origem."""
    used: Dict[str, int] = defaultdict(int)
    output: List[str] = []
    for raw_name in columns:
        name = sanitize_identifier(raw_name)
        if name in RESERVED_COLUMNS:
            name = f"orig_{name}"
        used[name] += 1
        output.append(name if used[name] == 1 else f"{name}_{used[name]}")
    return output


def source_year(file_name: str) -> Optional[int]:
    """Retorna o primeiro ano de quatro dígitos encontrado no nome do arquivo."""
    match = YEAR_PATTERN.search(file_name)
    return int(match.group(0)) if match else None


def group_key(file_name: str) -> str:
    """Remove anos do nome do arquivo e produz a chave do conjunto de dados."""
    stem = PurePosixPath(file_name).stem
    without_year = YEAR_PATTERN.sub("", stem)
    without_year = re.sub(r"[_\- ]{2,}", "_", without_year).strip("_ -")
    return sanitize_identifier(without_year, fallback="dataset_sem_nome")


def is_dictionary_file(file_name: str) -> bool:
    return bool(DICTIONARY_NAME_PATTERN.search(PurePosixPath(file_name).stem))


def list_files_recursively(path: str) -> List[Dict[str, Any]]:
    """Lista arquivos do Volume sem usar APIs externas ou tokens locais."""
    entries: List[Dict[str, Any]] = []
    pending = [path.rstrip("/")]
    while pending:
        current = pending.pop()
        for info in dbutils.fs.ls(current):  # noqa: F821 - disponível no Databricks
            item_path = info.path
            if item_path.endswith("/"):
                pending.append(item_path.rstrip("/"))
                continue
            suffix = PurePosixPath(item_path).suffix.lower()
            if suffix in ELIGIBLE_EXTENSIONS:
                entries.append(
                    {
                        "path": item_path,
                        "name": PurePosixPath(item_path).name,
                        "extension": suffix,
                        "size_bytes": int(getattr(info, "size", 0) or 0),
                        "modification_time": int(getattr(info, "modificationTime", 0) or 0),
                    }
                )
    return sorted(entries, key=lambda x: x["path"].lower())


def infer_csv_delimiter(path: str) -> str:
    """Escolhe o delimitador predominante na primeira linha não vazia do CSV."""
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
    delimiter = infer_csv_delimiter(path)
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("sep", delimiter)
        .option("mode", "PERMISSIVE")
        .load(path)
    )


def read_xlsx(path: str, file_name: str) -> List[Tuple[str, DataFrame]]:
    """Lê todas as abas de dados do XLSX pelo driver, sem dependência de conector Spark."""
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("A leitura XLSX requer pandas disponível no cluster Databricks.") from exc

    workbook = pd.ExcelFile(path, engine="openpyxl")
    dataframes: List[Tuple[str, DataFrame]] = []
    for sheet in workbook.sheet_names:
        pdf = pd.read_excel(workbook, sheet_name=sheet, dtype=object)
        pdf = pdf.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if pdf.empty:
            continue
        pdf.columns = unique_column_names([str(c) for c in pdf.columns])
        # Spark não aceita valores pandas NA/NaN como tipos heterogêneos.
        pdf = pdf.where(pd.notnull(pdf), None)
        dataframes.append((str(sheet), spark.createDataFrame(pdf)))
    if not dataframes:
        raise ValueError(f"Nenhuma aba com dados foi encontrada em {file_name}.")
    return dataframes


def standardize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*unique_column_names(df.columns))


def add_source_metadata(df: DataFrame, file_info: Dict[str, Any], sheet_name: Optional[str]) -> DataFrame:
    file_name = file_info["name"]
    year = source_year(file_name)
    modification_time = file_info.get("modification_time")
    modification_iso = None
    if modification_time:
        modification_iso = datetime.fromtimestamp(modification_time / 1000, tz=timezone.utc).isoformat()

    result = standardize_columns(df)
    result = (
        result.withColumn("ano", F.lit(year).cast("int"))
        .withColumn("arquivo_origem", F.lit(file_name))
        .withColumn("caminho_origem", F.lit(file_info["path"]))
        .withColumn("aba_origem", F.lit(sheet_name))
        .withColumn("data_modificacao_origem", F.lit(modification_iso).cast("string"))
        .withColumn("data_ingestao_utc", F.lit(RUN_TS))
    )
    return result


# -----------------------------------------------------------------------------
# Dicionários auxiliares existentes nos próprios arquivos
# -----------------------------------------------------------------------------
def read_dictionary_rows(file_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extrai descrições quando o nome do arquivo ou da aba indica dicionário/metadados."""
    if not is_dictionary_file(file_info["name"]):
        return []
    try:
        import pandas as pd
    except ImportError:
        return []

    rows: List[Dict[str, Any]] = []
    path = file_info["path"]
    try:
        if file_info["extension"] == ".csv":
            delimiter = infer_csv_delimiter(path)
            sheets = [("csv", pd.read_csv(path, sep=delimiter, dtype=str))]
        else:
            workbook = pd.ExcelFile(path, engine="openpyxl")
            sheets = [(str(s), pd.read_excel(workbook, sheet_name=s, dtype=str)) for s in workbook.sheet_names]
    except Exception:
        return []

    for sheet_name, pdf in sheets:
        if pdf.empty:
            continue
        normalized = {normalize_text(column): column for column in pdf.columns}
        field_column = next(
            (column for key, column in normalized.items() if any(token in key for token in ["campo", "field", "coluna", "atributo", "nome"])),
            None,
        )
        description_column = next(
            (
                column
                for key, column in normalized.items()
                if any(token in key for token in ["descricao", "description", "finalidade", "significado", "observacao"])
            ),
            None,
        )
        table_column = next(
            (column for key, column in normalized.items() if any(token in key for token in ["tabela", "dataset", "arquivo", "table"])),
            None,
        )
        if not field_column or not description_column:
            continue
        for _, record in pdf.iterrows():
            field = record.get(field_column)
            description = record.get(description_column)
            if pd.isna(field) or pd.isna(description):
                continue
            rows.append(
                {
                    "tabela_referencia": sanitize_identifier(record.get(table_column)) if table_column else None,
                    "campo_referencia": sanitize_identifier(field),
                    "descricao": str(description).strip(),
                    "arquivo_dicionario_origem": file_info["name"],
                    "aba_dicionario_origem": sheet_name,
                }
            )
    return rows


def build_dictionary_lookup(dictionary_rows: Iterable[Dict[str, Any]]) -> Dict[Tuple[Optional[str], str], str]:
    lookup: Dict[Tuple[Optional[str], str], str] = {}
    for row in dictionary_rows:
        key = (row.get("tabela_referencia"), row["campo_referencia"])
        lookup[key] = row["descricao"]
        lookup[(None, row["campo_referencia"])] = row["descricao"]
    return lookup


# -----------------------------------------------------------------------------
# Duplicidades e gravação Delta
# -----------------------------------------------------------------------------
def add_duplicate_indices(df: DataFrame) -> DataFrame:
    """Cria chave de duplicidade e índice sequencial por grupo de registros iguais."""
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


def write_delta_table(df: DataFrame, table_name: str, table_path: str) -> None:
    """Registra a tabela Delta no catálogo Bronze e mantém seu caminho no Volume."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", table_path)
        .saveAsTable(table_name)
    )


def spark_sql_type(data_type: T.DataType) -> str:
    return data_type.simpleString()


def description_for(table_key: str, field: str, lookup: Dict[Tuple[Optional[str], str], str]) -> str:
    return lookup.get((table_key, field), lookup.get((None, field), f"Campo carregado da fonte do conjunto {table_key}."))


# -----------------------------------------------------------------------------
# Execução principal
# -----------------------------------------------------------------------------
all_files = list_files_recursively(LANDING_ZONE)
if not all_files:
    raise FileNotFoundError(f"Nenhum arquivo CSV/XLSX foi encontrado em {LANDING_ZONE}.")

files_by_group: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
dictionary_rows: List[Dict[str, Any]] = []
for file_info in all_files:
    files_by_group[group_key(file_info["name"])].append(file_info)
    dictionary_rows.extend(read_dictionary_rows(file_info))

dictionary_lookup = build_dictionary_lookup(dictionary_rows)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

run_summary: List[Dict[str, Any]] = []
dictionary_output: List[Dict[str, Any]] = []

for dataset_key, group_files in sorted(files_by_group.items()):
    table_name = f"{BRONZE_SCHEMA}.{sanitize_identifier(dataset_key)}"
    table_path = f"{BRONZE_ROOT.rstrip('/')}/{sanitize_identifier(dataset_key)}"
    frames: List[DataFrame] = []
    source_names: List[str] = []
    try:
        for file_info in group_files:
            source_names.append(file_info["name"])
            if file_info["extension"] == ".csv":
                frames.append(add_source_metadata(read_csv(file_info["path"]), file_info, None))
            elif file_info["extension"] == ".xlsx":
                for sheet_name, sheet_df in read_xlsx(file_info["path"], file_info["name"]):
                    frames.append(add_source_metadata(sheet_df, file_info, sheet_name))

        if not frames:
            continue
        combined = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)
        bronze_df = add_duplicate_indices(combined)
        write_delta_table(bronze_df, table_name, table_path)
        row_count = bronze_df.count()

        for field in bronze_df.schema.fields:
            dictionary_output.append(
                {
                    "nome_tabela": table_name,
                    "nome_dataset": dataset_key,
                    "campo": field.name,
                    "tipo_spark": spark_sql_type(field.dataType),
                    "tipo_sql": field.dataType.simpleString(),
                    "nullable": bool(field.nullable),
                    "finalidade_descricao": description_for(dataset_key, field.name, dictionary_lookup),
                    "origens": ", ".join(source_names),
                    "camada": "bronze",
                    "caminho_tabela": table_path,
                    "data_atualizacao_utc": RUN_TS,
                }
            )
        run_summary.append(
            {
                "nome_dataset": dataset_key,
                "nome_tabela": table_name,
                "arquivos_processados": ", ".join(source_names),
                "quantidade_arquivos": len(source_names),
                "quantidade_registros": row_count,
                "status": "SUCESSO",
                "mensagem": None,
                "data_execucao_utc": RUN_TS,
            }
        )
    except Exception as exc:
        run_summary.append(
            {
                "nome_dataset": dataset_key,
                "nome_tabela": table_name,
                "arquivos_processados": ", ".join(source_names),
                "quantidade_arquivos": len(source_names),
                "quantidade_registros": None,
                "status": "ERRO",
                "mensagem": repr(exc)[:4000],
                "data_execucao_utc": RUN_TS,
            }
        )

if dictionary_output:
    dictionary_schema = T.StructType(
        [
            T.StructField("nome_tabela", T.StringType(), False),
            T.StructField("nome_dataset", T.StringType(), False),
            T.StructField("campo", T.StringType(), False),
            T.StructField("tipo_spark", T.StringType(), False),
            T.StructField("tipo_sql", T.StringType(), False),
            T.StructField("nullable", T.BooleanType(), True),
            T.StructField("finalidade_descricao", T.StringType(), True),
            T.StructField("origens", T.StringType(), True),
            T.StructField("camada", T.StringType(), False),
            T.StructField("caminho_tabela", T.StringType(), True),
            T.StructField("data_atualizacao_utc", T.StringType(), False),
        ]
    )
    dictionary_df = spark.createDataFrame(dictionary_output, schema=dictionary_schema)
    write_delta_table(
        dictionary_df,
        DICTIONARY_TABLE,
        f"{BRONZE_ROOT.rstrip('/')}/dicionario_dados_bronze",
    )

if run_summary:
    audit_schema = T.StructType(
        [
            T.StructField("nome_dataset", T.StringType(), False),
            T.StructField("nome_tabela", T.StringType(), False),
            T.StructField("arquivos_processados", T.StringType(), True),
            T.StructField("quantidade_arquivos", T.IntegerType(), True),
            T.StructField("quantidade_registros", T.LongType(), True),
            T.StructField("status", T.StringType(), False),
            T.StructField("mensagem", T.StringType(), True),
            T.StructField("data_execucao_utc", T.StringType(), False),
        ]
    )
    audit_df = spark.createDataFrame(run_summary, schema=audit_schema)
    write_delta_table(audit_df, AUDIT_TABLE, f"{BRONZE_ROOT.rstrip('/')}/controle_carga_bronze")

# Exibe resumo no notebook e facilita validação da execução.
display(spark.createDataFrame(run_summary))
=======
# MAGIC
# MAGIC from __future__ import annotations
# MAGIC
# MAGIC import json
# MAGIC import re
# MAGIC import unicodedata
# MAGIC from collections import defaultdict
# MAGIC from datetime import datetime, timezone
# MAGIC from functools import reduce
# MAGIC from pathlib import PurePosixPath
# MAGIC from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
# MAGIC
# MAGIC from pyspark.sql import DataFrame, Row, SparkSession, Window
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC
# MAGIC
# MAGIC # -----------------------------------------------------------------------------
# MAGIC # Parâmetros do processo
# MAGIC # -----------------------------------------------------------------------------
# MAGIC LANDING_ZONE = "/Volumes/handson_beta/landing_zone/arquivos/"
# MAGIC BRONZE_ROOT = "/Volumes/handson_beta/prata/bronze/"
# MAGIC BRONZE_SCHEMA = "handson_beta.bronze"
# MAGIC DICTIONARY_TABLE = f"{BRONZE_SCHEMA}.dicionario_dados_bronze"
# MAGIC AUDIT_TABLE = f"{BRONZE_SCHEMA}.controle_carga_bronze"
# MAGIC RUN_TS = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
# MAGIC
# MAGIC ELIGIBLE_EXTENSIONS = {".csv", ".xlsx"}
# MAGIC YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
# MAGIC DICTIONARY_NAME_PATTERN = re.compile(
# MAGIC     r"(dicionario|dicionário|dictionary|metadado|metadata|layout|legenda|descricao|descrição)",
# MAGIC     flags=re.IGNORECASE,
# MAGIC )
# MAGIC RESERVED_COLUMNS = {
# MAGIC     "ano",
# MAGIC     "arquivo_origem",
# MAGIC     "caminho_origem",
# MAGIC     "aba_origem",
# MAGIC     "data_modificacao_origem",
# MAGIC     "chave_duplicidade",
# MAGIC     "indice_duplicidade",
# MAGIC     "quantidade_duplicidade",
# MAGIC     "registro_duplicado",
# MAGIC     "data_ingestao_utc",
# MAGIC }
# MAGIC
# MAGIC spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC
# MAGIC # -----------------------------------------------------------------------------
# MAGIC # Utilitários de nomes, arquivos e esquemas
# MAGIC # -----------------------------------------------------------------------------
# MAGIC def normalize_text(value: Any) -> str:
# MAGIC     """Normaliza texto para comparação de nomes de tabelas e campos."""
# MAGIC     text = "" if value is None else str(value)
# MAGIC     text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
# MAGIC     return text.lower().strip()
# MAGIC
# MAGIC
# MAGIC def sanitize_identifier(value: Any, fallback: str = "campo") -> str:
# MAGIC     """Converte nomes de origem em identificadores Spark/SQL estáveis."""
# MAGIC     text = normalize_text(value)
# MAGIC     text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
# MAGIC     if not text:
# MAGIC         text = fallback
# MAGIC     if text[0].isdigit():
# MAGIC         text = f"c_{text}"
# MAGIC     return text[:250]
# MAGIC
# MAGIC
# MAGIC def unique_column_names(columns: Sequence[str]) -> List[str]:
# MAGIC     """Garante nomes únicos, preservando a ordem das colunas de origem."""
# MAGIC     used: Dict[str, int] = defaultdict(int)
# MAGIC     output: List[str] = []
# MAGIC     for raw_name in columns:
# MAGIC         name = sanitize_identifier(raw_name)
# MAGIC         if name in RESERVED_COLUMNS:
# MAGIC             name = f"orig_{name}"
# MAGIC         used[name] += 1
# MAGIC         output.append(name if used[name] == 1 else f"{name}_{used[name]}")
# MAGIC     return output
# MAGIC
# MAGIC
# MAGIC def source_year(file_name: str) -> Optional[int]:
# MAGIC     """Retorna o primeiro ano de quatro dígitos encontrado no nome do arquivo."""
# MAGIC     match = YEAR_PATTERN.search(file_name)
# MAGIC     return int(match.group(0)) if match else None
# MAGIC
# MAGIC
# MAGIC def group_key(file_name: str) -> str:
# MAGIC     """Remove anos do nome do arquivo e produz a chave do conjunto de dados."""
# MAGIC     stem = PurePosixPath(file_name).stem
# MAGIC     without_year = YEAR_PATTERN.sub("", stem)
# MAGIC     without_year = re.sub(r"[_\- ]{2,}", "_", without_year).strip("_ -")
# MAGIC     return sanitize_identifier(without_year, fallback="dataset_sem_nome")
# MAGIC
# MAGIC
# MAGIC def is_dictionary_file(file_name: str) -> bool:
# MAGIC     return bool(DICTIONARY_NAME_PATTERN.search(PurePosixPath(file_name).stem))
# MAGIC
# MAGIC
# MAGIC def list_files_recursively(path: str) -> List[Dict[str, Any]]:
# MAGIC     """Lista arquivos do Volume sem usar APIs externas ou tokens locais."""
# MAGIC     entries: List[Dict[str, Any]] = []
# MAGIC     pending = [path.rstrip("/")]
# MAGIC     while pending:
# MAGIC         current = pending.pop()
# MAGIC         for info in dbutils.fs.ls(current):  # noqa: F821 - disponível no Databricks
# MAGIC             item_path = info.path
# MAGIC             if item_path.endswith("/"):
# MAGIC                 pending.append(item_path.rstrip("/"))
# MAGIC                 continue
# MAGIC             suffix = PurePosixPath(item_path).suffix.lower()
# MAGIC             if suffix in ELIGIBLE_EXTENSIONS:
# MAGIC                 entries.append(
# MAGIC                     {
# MAGIC                         "path": item_path,
# MAGIC                         "name": PurePosixPath(item_path).name,
# MAGIC                         "extension": suffix,
# MAGIC                         "size_bytes": int(getattr(info, "size", 0) or 0),
# MAGIC                         "modification_time": int(getattr(info, "modificationTime", 0) or 0),
# MAGIC                     }
# MAGIC                 )
# MAGIC     return sorted(entries, key=lambda x: x["path"].lower())
# MAGIC
# MAGIC
# MAGIC def infer_csv_delimiter(path: str) -> str:
# MAGIC     """Escolhe o delimitador predominante na primeira linha não vazia do CSV."""
# MAGIC     try:
# MAGIC         sample = dbutils.fs.head(path, 65536)  # noqa: F821 - disponível no Databricks
# MAGIC     except Exception:
# MAGIC         return ","
# MAGIC     lines = [line for line in sample.splitlines() if line.strip()]
# MAGIC     if not lines:
# MAGIC         return ","
# MAGIC     header = lines[0]
# MAGIC     candidates = {delimiter: header.count(delimiter) for delimiter in [",", ";", "\t", "|"]}
# MAGIC     delimiter, count = max(candidates.items(), key=lambda item: item[1])
# MAGIC     return delimiter if count > 0 else ","
# MAGIC
# MAGIC
# MAGIC def read_csv(path: str) -> DataFrame:
# MAGIC     delimiter = infer_csv_delimiter(path)
# MAGIC     return (
# MAGIC         spark.read.format("csv")
# MAGIC         .option("header", "true")
# MAGIC         .option("inferSchema", "true")
# MAGIC         .option("multiLine", "true")
# MAGIC         .option("quote", '"')
# MAGIC         .option("escape", '"')
# MAGIC         .option("encoding", "UTF-8")
# MAGIC         .option("sep", delimiter)
# MAGIC         .option("mode", "PERMISSIVE")
# MAGIC         .load(path)
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def read_xlsx(path: str, file_name: str) -> List[Tuple[str, DataFrame]]:
# MAGIC     """Lê todas as abas de dados do XLSX pelo driver, sem dependência de conector Spark."""
# MAGIC     try:
# MAGIC         import pandas as pd
# MAGIC     except ImportError as exc:
# MAGIC         raise RuntimeError("A leitura XLSX requer pandas disponível no cluster Databricks.") from exc
# MAGIC
# MAGIC     workbook = pd.ExcelFile(path, engine="openpyxl")
# MAGIC     dataframes: List[Tuple[str, DataFrame]] = []
# MAGIC     for sheet in workbook.sheet_names:
# MAGIC         pdf = pd.read_excel(workbook, sheet_name=sheet, dtype=object)
# MAGIC         pdf = pdf.dropna(axis=0, how="all").dropna(axis=1, how="all")
# MAGIC         if pdf.empty:
# MAGIC             continue
# MAGIC         pdf.columns = unique_column_names([str(c) for c in pdf.columns])
# MAGIC         # Spark não aceita valores pandas NA/NaN como tipos heterogêneos.
# MAGIC         pdf = pdf.where(pd.notnull(pdf), None)
# MAGIC         dataframes.append((str(sheet), spark.createDataFrame(pdf)))
# MAGIC     if not dataframes:
# MAGIC         raise ValueError(f"Nenhuma aba com dados foi encontrada em {file_name}.")
# MAGIC     return dataframes
# MAGIC
# MAGIC
# MAGIC def standardize_columns(df: DataFrame) -> DataFrame:
# MAGIC     return df.toDF(*unique_column_names(df.columns))
# MAGIC
# MAGIC
# MAGIC def add_source_metadata(df: DataFrame, file_info: Dict[str, Any], sheet_name: Optional[str]) -> DataFrame:
# MAGIC     file_name = file_info["name"]
# MAGIC     year = source_year(file_name)
# MAGIC     modification_time = file_info.get("modification_time")
# MAGIC     modification_iso = None
# MAGIC     if modification_time:
# MAGIC         modification_iso = datetime.fromtimestamp(modification_time / 1000, tz=timezone.utc).isoformat()
# MAGIC
# MAGIC     result = standardize_columns(df)
# MAGIC     result = (
# MAGIC         result.withColumn("ano", F.lit(year).cast("int"))
# MAGIC         .withColumn("arquivo_origem", F.lit(file_name))
# MAGIC         .withColumn("caminho_origem", F.lit(file_info["path"]))
# MAGIC         .withColumn("aba_origem", F.lit(sheet_name))
# MAGIC         .withColumn("data_modificacao_origem", F.lit(modification_iso).cast("string"))
# MAGIC         .withColumn("data_ingestao_utc", F.lit(RUN_TS))
# MAGIC     )
# MAGIC     return result
# MAGIC
# MAGIC
# MAGIC # -----------------------------------------------------------------------------
# MAGIC # Dicionários auxiliares existentes nos próprios arquivos
# MAGIC # -----------------------------------------------------------------------------
# MAGIC def read_dictionary_rows(file_info: Dict[str, Any]) -> List[Dict[str, Any]]:
# MAGIC     """Extrai descrições quando o nome do arquivo ou da aba indica dicionário/metadados."""
# MAGIC     if not is_dictionary_file(file_info["name"]):
# MAGIC         return []
# MAGIC     try:
# MAGIC         import pandas as pd
# MAGIC     except ImportError:
# MAGIC         return []
# MAGIC
# MAGIC     rows: List[Dict[str, Any]] = []
# MAGIC     path = file_info["path"]
# MAGIC     try:
# MAGIC         if file_info["extension"] == ".csv":
# MAGIC             delimiter = infer_csv_delimiter(path)
# MAGIC             sheets = [("csv", pd.read_csv(path, sep=delimiter, dtype=str))]
# MAGIC         else:
# MAGIC             workbook = pd.ExcelFile(path, engine="openpyxl")
# MAGIC             sheets = [(str(s), pd.read_excel(workbook, sheet_name=s, dtype=str)) for s in workbook.sheet_names]
# MAGIC     except Exception:
# MAGIC         return []
# MAGIC
# MAGIC     for sheet_name, pdf in sheets:
# MAGIC         if pdf.empty:
# MAGIC             continue
# MAGIC         normalized = {normalize_text(column): column for column in pdf.columns}
# MAGIC         field_column = next(
# MAGIC             (column for key, column in normalized.items() if any(token in key for token in ["campo", "field", "coluna", "atributo", "nome"])),
# MAGIC             None,
# MAGIC         )
# MAGIC         description_column = next(
# MAGIC             (
# MAGIC                 column
# MAGIC                 for key, column in normalized.items()
# MAGIC                 if any(token in key for token in ["descricao", "description", "finalidade", "significado", "observacao"])
# MAGIC             ),
# MAGIC             None,
# MAGIC         )
# MAGIC         table_column = next(
# MAGIC             (column for key, column in normalized.items() if any(token in key for token in ["tabela", "dataset", "arquivo", "table"])),
# MAGIC             None,
# MAGIC         )
# MAGIC         if not field_column or not description_column:
# MAGIC             continue
# MAGIC         for _, record in pdf.iterrows():
# MAGIC             field = record.get(field_column)
# MAGIC             description = record.get(description_column)
# MAGIC             if pd.isna(field) or pd.isna(description):
# MAGIC                 continue
# MAGIC             rows.append(
# MAGIC                 {
# MAGIC                     "tabela_referencia": sanitize_identifier(record.get(table_column)) if table_column else None,
# MAGIC                     "campo_referencia": sanitize_identifier(field),
# MAGIC                     "descricao": str(description).strip(),
# MAGIC                     "arquivo_dicionario_origem": file_info["name"],
# MAGIC                     "aba_dicionario_origem": sheet_name,
# MAGIC                 }
# MAGIC             )
# MAGIC     return rows
# MAGIC
# MAGIC
# MAGIC def build_dictionary_lookup(dictionary_rows: Iterable[Dict[str, Any]]) -> Dict[Tuple[Optional[str], str], str]:
# MAGIC     lookup: Dict[Tuple[Optional[str], str], str] = {}
# MAGIC     for row in dictionary_rows:
# MAGIC         key = (row.get("tabela_referencia"), row["campo_referencia"])
# MAGIC         lookup[key] = row["descricao"]
# MAGIC         lookup[(None, row["campo_referencia"])] = row["descricao"]
# MAGIC     return lookup
# MAGIC
# MAGIC
# MAGIC # -----------------------------------------------------------------------------
# MAGIC # Duplicidades e gravação Delta
# MAGIC # -----------------------------------------------------------------------------
# MAGIC def add_duplicate_indices(df: DataFrame) -> DataFrame:
# MAGIC     """Cria chave de duplicidade e índice sequencial por grupo de registros iguais."""
# MAGIC     source_columns = [
# MAGIC         column
# MAGIC         for column in df.columns
# MAGIC         if column
# MAGIC         not in {
# MAGIC             "arquivo_origem",
# MAGIC             "caminho_origem",
# MAGIC             "aba_origem",
# MAGIC             "data_modificacao_origem",
# MAGIC             "data_ingestao_utc",
# MAGIC         }
# MAGIC     ]
# MAGIC     if not source_columns:
# MAGIC         source_columns = ["arquivo_origem"]
# MAGIC
# MAGIC     hash_parts = [F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in source_columns]
# MAGIC     result = df.withColumn("chave_duplicidade", F.sha2(F.concat_ws("||", *hash_parts), 256))
# MAGIC     duplicate_window = Window.partitionBy("chave_duplicidade").orderBy(
# MAGIC         F.col("ano").asc_nulls_last(),
# MAGIC         F.col("arquivo_origem").asc(),
# MAGIC         F.col("caminho_origem").asc(),
# MAGIC         F.col("aba_origem").asc_nulls_last(),
# MAGIC     )
# MAGIC     count_window = Window.partitionBy("chave_duplicidade")
# MAGIC     return (
# MAGIC         result.withColumn("quantidade_duplicidade", F.count(F.lit(1)).over(count_window))
# MAGIC         .withColumn("indice_duplicidade", F.row_number().over(duplicate_window))
# MAGIC         .withColumn("registro_duplicado", F.col("quantidade_duplicidade") > 1)
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def write_delta_table(df: DataFrame, table_name: str, table_path: str) -> None:
# MAGIC     """Registra a tabela Delta no catálogo Bronze e mantém seu caminho no Volume."""
# MAGIC     (
# MAGIC         df.write.format("delta")
# MAGIC         .mode("overwrite")
# MAGIC         .option("overwriteSchema", "true")
# MAGIC         .option("path", table_path)
# MAGIC         .saveAsTable(table_name)
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def spark_sql_type(data_type: T.DataType) -> str:
# MAGIC     return data_type.simpleString()
# MAGIC
# MAGIC
# MAGIC def description_for(table_key: str, field: str, lookup: Dict[Tuple[Optional[str], str], str]) -> str:
# MAGIC     return lookup.get((table_key, field), lookup.get((None, field), f"Campo carregado da fonte do conjunto {table_key}."))
# MAGIC
# MAGIC
# MAGIC # -----------------------------------------------------------------------------
# MAGIC # Execução principal
# MAGIC # -----------------------------------------------------------------------------
# MAGIC all_files = list_files_recursively(LANDING_ZONE)
# MAGIC if not all_files:
# MAGIC     raise FileNotFoundError(f"Nenhum arquivo CSV/XLSX foi encontrado em {LANDING_ZONE}.")
# MAGIC
# MAGIC files_by_group: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
# MAGIC dictionary_rows: List[Dict[str, Any]] = []
# MAGIC for file_info in all_files:
# MAGIC     files_by_group[group_key(file_info["name"])].append(file_info)
# MAGIC     dictionary_rows.extend(read_dictionary_rows(file_info))
# MAGIC
# MAGIC dictionary_lookup = build_dictionary_lookup(dictionary_rows)
# MAGIC
# MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
# MAGIC
# MAGIC run_summary: List[Dict[str, Any]] = []
# MAGIC dictionary_output: List[Dict[str, Any]] = []
# MAGIC
# MAGIC for dataset_key, group_files in sorted(files_by_group.items()):
# MAGIC     table_name = f"{BRONZE_SCHEMA}.{sanitize_identifier(dataset_key)}"
# MAGIC     table_path = f"{BRONZE_ROOT.rstrip('/')}/{sanitize_identifier(dataset_key)}"
# MAGIC     frames: List[DataFrame] = []
# MAGIC     source_names: List[str] = []
# MAGIC     try:
# MAGIC         for file_info in group_files:
# MAGIC             source_names.append(file_info["name"])
# MAGIC             if file_info["extension"] == ".csv":
# MAGIC                 frames.append(add_source_metadata(read_csv(file_info["path"]), file_info, None))
# MAGIC             elif file_info["extension"] == ".xlsx":
# MAGIC                 for sheet_name, sheet_df in read_xlsx(file_info["path"], file_info["name"]):
# MAGIC                     frames.append(add_source_metadata(sheet_df, file_info, sheet_name))
# MAGIC
# MAGIC         if not frames:
# MAGIC             continue
# MAGIC         combined = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)
# MAGIC         bronze_df = add_duplicate_indices(combined)
# MAGIC         write_delta_table(bronze_df, table_name, table_path)
# MAGIC         row_count = bronze_df.count()
# MAGIC
# MAGIC         for field in bronze_df.schema.fields:
# MAGIC             dictionary_output.append(
# MAGIC                 {
# MAGIC                     "nome_tabela": table_name,
# MAGIC                     "nome_dataset": dataset_key,
# MAGIC                     "campo": field.name,
# MAGIC                     "tipo_spark": spark_sql_type(field.dataType),
# MAGIC                     "tipo_sql": field.dataType.simpleString(),
# MAGIC                     "nullable": bool(field.nullable),
# MAGIC                     "finalidade_descricao": description_for(dataset_key, field.name, dictionary_lookup),
# MAGIC                     "origens": ", ".join(source_names),
# MAGIC                     "camada": "bronze",
# MAGIC                     "caminho_tabela": table_path,
# MAGIC                     "data_atualizacao_utc": RUN_TS,
# MAGIC                 }
# MAGIC             )
# MAGIC         run_summary.append(
# MAGIC             {
# MAGIC                 "nome_dataset": dataset_key,
# MAGIC                 "nome_tabela": table_name,
# MAGIC                 "arquivos_processados": ", ".join(source_names),
# MAGIC                 "quantidade_arquivos": len(source_names),
# MAGIC                 "quantidade_registros": row_count,
# MAGIC                 "status": "SUCESSO",
# MAGIC                 "mensagem": None,
# MAGIC                 "data_execucao_utc": RUN_TS,
# MAGIC             }
# MAGIC         )
# MAGIC     except Exception as exc:
# MAGIC         run_summary.append(
# MAGIC             {
# MAGIC                 "nome_dataset": dataset_key,
# MAGIC                 "nome_tabela": table_name,
# MAGIC                 "arquivos_processados": ", ".join(source_names),
# MAGIC                 "quantidade_arquivos": len(source_names),
# MAGIC                 "quantidade_registros": None,
# MAGIC                 "status": "ERRO",
# MAGIC                 "mensagem": repr(exc)[:4000],
# MAGIC                 "data_execucao_utc": RUN_TS,
# MAGIC             }
# MAGIC         )
# MAGIC
# MAGIC if dictionary_output:
# MAGIC     dictionary_schema = T.StructType(
# MAGIC         [
# MAGIC             T.StructField("nome_tabela", T.StringType(), False),
# MAGIC             T.StructField("nome_dataset", T.StringType(), False),
# MAGIC             T.StructField("campo", T.StringType(), False),
# MAGIC             T.StructField("tipo_spark", T.StringType(), False),
# MAGIC             T.StructField("tipo_sql", T.StringType(), False),
# MAGIC             T.StructField("nullable", T.BooleanType(), True),
# MAGIC             T.StructField("finalidade_descricao", T.StringType(), True),
# MAGIC             T.StructField("origens", T.StringType(), True),
# MAGIC             T.StructField("camada", T.StringType(), False),
# MAGIC             T.StructField("caminho_tabela", T.StringType(), True),
# MAGIC             T.StructField("data_atualizacao_utc", T.StringType(), False),
# MAGIC         ]
# MAGIC     )
# MAGIC     dictionary_df = spark.createDataFrame(dictionary_output, schema=dictionary_schema)
# MAGIC     write_delta_table(
# MAGIC         dictionary_df,
# MAGIC         DICTIONARY_TABLE,
# MAGIC         f"{BRONZE_ROOT.rstrip('/')}/dicionario_dados_bronze",
# MAGIC     )
# MAGIC
# MAGIC if run_summary:
# MAGIC     audit_schema = T.StructType(
# MAGIC         [
# MAGIC             T.StructField("nome_dataset", T.StringType(), False),
# MAGIC             T.StructField("nome_tabela", T.StringType(), False),
# MAGIC             T.StructField("arquivos_processados", T.StringType(), True),
# MAGIC             T.StructField("quantidade_arquivos", T.IntegerType(), True),
# MAGIC             T.StructField("quantidade_registros", T.LongType(), True),
# MAGIC             T.StructField("status", T.StringType(), False),
# MAGIC             T.StructField("mensagem", T.StringType(), True),
# MAGIC             T.StructField("data_execucao_utc", T.StringType(), False),
# MAGIC         ]
# MAGIC     )
# MAGIC     audit_df = spark.createDataFrame(run_summary, schema=audit_schema)
# MAGIC     write_delta_table(audit_df, AUDIT_TABLE, f"{BRONZE_ROOT.rstrip('/')}/controle_carga_bronze")
# MAGIC
# MAGIC # Exibe resumo no notebook e facilita validação da execução.
# MAGIC display(spark.createDataFrame(run_summary))
>>>>>>> Stashed changes
