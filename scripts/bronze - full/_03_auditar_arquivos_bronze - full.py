"""Auditoria de arquivos da Landing Zone versus a ingestão Bronze.

O script deve ser executado como Python file no Databricks. Ele:

1. lista recursivamente todos os arquivos da Landing Zone;
2. classifica extensões elegíveis (.csv e .xlsx) e não elegíveis;
3. lê a tabela de auditoria da ingestão Bronze;
4. compara cada arquivo com os conjuntos registrados na auditoria;
5. grava uma tabela completa de reconciliação;
6. grava uma tabela somente com arquivos não considerados; e
7. grava um resumo quantitativo por status.

A auditoria utiliza o nome do arquivo e o nome normalizado do conjunto de dados,
porque a tabela de controle_carga_bronze registra os nomes dos arquivos, mas não
mantém uma lista JSON de caminhos individuais.
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------------------------------------------------------
# Parâmetros
# -----------------------------------------------------------------------------
LANDING_ZONE = "/Volumes/handson_beta/landing_zone/arquivos/"
BRONZE_CATALOG = "handson_beta"
BRONZE_METADATA_SCHEMA = f"{BRONZE_CATALOG}.bronze_meta"
AUDIT_TABLE = f"{BRONZE_METADATA_SCHEMA}.controle_carga_bronze"
RECONCILIATION_TABLE = f"{BRONZE_METADATA_SCHEMA}.controle_arquivos_bronze"
NOT_CONSIDERED_TABLE = f"{BRONZE_METADATA_SCHEMA}.arquivos_bronze_nao_considerados"
SUMMARY_TABLE = f"{BRONZE_METADATA_SCHEMA}.resumo_reconciliacao_bronze"
ELIGIBLE_EXTENSIONS = {".csv", ".xlsx"}
YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
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


def dataset_key(file_name: str) -> str:
    stem = PurePosixPath(file_name).stem
    stem_without_year = YEAR_PATTERN.sub("", stem)
    stem_without_year = re.sub(r"[_\- ]{2,}", "_", stem_without_year).strip("_ -")
    return sanitize_identifier(stem_without_year)


def list_files_recursively(path: str) -> List[Dict[str, Any]]:
    """Lista todos os arquivos do Volume, inclusive os não elegíveis."""
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
            entries.append(
                {
                    "arquivo_origem": PurePosixPath(item_path).name,
                    "caminho_origem": item_path,
                    "extensao": suffix or None,
                    "tamanho_bytes": int(getattr(info, "size", 0) or 0),
                    "data_modificacao_ms": int(getattr(info, "modificationTime", 0) or 0),
                    "nome_dataset": dataset_key(PurePosixPath(item_path).name),
                }
            )
    return sorted(entries, key=lambda item: item["caminho_origem"].lower())


def table_exists(table_name: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table_name))
    except Exception:
        try:
            spark.table(table_name).limit(0).count()
            return True
        except Exception:
            return False


def audit_index_from_table() -> Tuple[DefaultDict[Tuple[str, str], List[Dict[str, Any]]], Optional[str]]:
    """Cria índice por dataset e nome de arquivo a partir da auditoria Bronze."""
    index: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    if not table_exists(AUDIT_TABLE):
        return index, f"Tabela de auditoria inexistente: {AUDIT_TABLE}"

    audit_df = spark.table(AUDIT_TABLE)
    expected_columns = {
        "nome_dataset",
        "nome_tabela",
        "schema_bronze",
        "arquivos_processados",
        "status",
        "mensagem",
        "data_execucao_utc",
    }
    missing = expected_columns.difference(audit_df.columns)
    if missing:
        return index, f"Tabela de auditoria sem colunas esperadas: {sorted(missing)}"

    for row in audit_df.select(*sorted(expected_columns)).collect():
        data = row.asDict(recursive=True)
        raw_files = str(data.get("arquivos_processados") or "")
        for file_name in [item.strip() for item in raw_files.split(",") if item.strip()]:
            key = (str(data.get("nome_dataset") or ""), file_name)
            index[key].append(data)
    return index, None


def choose_audit_record(records: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    records = list(records)
    if not records:
        return None
    successful = [record for record in records if str(record.get("status") or "").upper() == "SUCESSO"]
    return successful[-1] if successful else records[-1]


def status_for_file(file_info: Dict[str, Any], records: List[Dict[str, Any]]) -> Tuple[str, str, Optional[Dict[str, Any]]]:
    extension = file_info["extensao"]
    if extension not in ELIGIBLE_EXTENSIONS:
        return "NAO_ELEGIVEL_EXTENSAO", "A extensão não faz parte do escopo CSV/XLSX desta ingestão.", None

    selected = choose_audit_record(records)
    if selected is None:
        return "NAO_CONSIDERADO_SEM_REGISTRO", "Arquivo elegível sem registro na tabela de auditoria Bronze.", None

    if str(selected.get("status") or "").upper() == "SUCESSO":
        return "CONSIDERADO_SUCESSO", "Arquivo elegível registrado em uma carga Bronze com sucesso.", selected

    error_message = selected.get("mensagem") or "A carga do conjunto foi registrada com erro."
    return "NAO_CONSIDERADO_ERRO_DE_CARGA", str(error_message)[:4000], selected


# -----------------------------------------------------------------------------
# Reconciliação
# -----------------------------------------------------------------------------
all_files = list_files_recursively(LANDING_ZONE)
audit_index, audit_problem = audit_index_from_table()

reconciliation_rows: List[Dict[str, Any]] = []
for file_info in all_files:
    key = (file_info["nome_dataset"], file_info["arquivo_origem"])
    records = audit_index.get(key, [])
    reconciliation_status, reason, audit_record = status_for_file(file_info, records)
    reconciliation_rows.append(
        {
            **file_info,
            "status_reconciliacao": reconciliation_status,
            "motivo": reason,
            "nome_tabela": audit_record.get("nome_tabela") if audit_record else None,
            "schema_bronze": audit_record.get("schema_bronze") if audit_record else None,
            "status_carga": audit_record.get("status") if audit_record else None,
            "mensagem_carga": audit_record.get("mensagem") if audit_record else None,
            "data_execucao_carga_utc": audit_record.get("data_execucao_utc") if audit_record else None,
            "data_reconciliacao_utc": RUN_TS,
            "problema_auditoria": audit_problem,
        }
    )

reconciliation_schema = T.StructType(
    [
        T.StructField("arquivo_origem", T.StringType(), False),
        T.StructField("caminho_origem", T.StringType(), False),
        T.StructField("extensao", T.StringType(), True),
        T.StructField("tamanho_bytes", T.LongType(), True),
        T.StructField("data_modificacao_ms", T.LongType(), True),
        T.StructField("nome_dataset", T.StringType(), False),
        T.StructField("status_reconciliacao", T.StringType(), False),
        T.StructField("motivo", T.StringType(), False),
        T.StructField("nome_tabela", T.StringType(), True),
        T.StructField("schema_bronze", T.StringType(), True),
        T.StructField("status_carga", T.StringType(), True),
        T.StructField("mensagem_carga", T.StringType(), True),
        T.StructField("data_execucao_carga_utc", T.StringType(), True),
        T.StructField("data_reconciliacao_utc", T.StringType(), False),
        T.StructField("problema_auditoria", T.StringType(), True),
    ]
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_METADATA_SCHEMA}")
reconciliation_df = spark.createDataFrame(reconciliation_rows, schema=reconciliation_schema)
reconciliation_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(RECONCILIATION_TABLE)

not_considered_df = reconciliation_df.filter(F.col("status_reconciliacao") != F.lit("CONSIDERADO_SUCESSO"))
not_considered_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(NOT_CONSIDERED_TABLE)

summary_df = (
    reconciliation_df.groupBy("status_reconciliacao")
    .agg(F.count(F.lit(1)).alias("quantidade_arquivos"))
    .withColumn("data_reconciliacao_utc", F.lit(RUN_TS))
    .orderBy("status_reconciliacao")
)
summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SUMMARY_TABLE)

print(f"Arquivos encontrados na Landing Zone: {len(all_files)}")
print(f"Arquivos não considerados: {not_considered_df.count()}")
print(f"Tabela completa: {RECONCILIATION_TABLE}")
print(f"Tabela de não considerados: {NOT_CONSIDERED_TABLE}")
print(f"Resumo: {SUMMARY_TABLE}")
summary_df.show(truncate=False)
not_considered_df.orderBy("status_reconciliacao", "caminho_origem").show(truncate=False)
