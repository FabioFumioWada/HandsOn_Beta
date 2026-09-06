#!/usr/bin/env python3
"""Provisiona a estrutura de catálogos do projeto HandsOn Beta no Databricks.

O script foi desenhado para ser executado localmente, em um job do Databricks ou
em um pipeline do GitHub Actions. A operação padrão é idempotente: somente cria
objetos ausentes. A remoção e recriação completas ficam bloqueadas por padrão.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

try:
    from databricks import sql
except ImportError as exc:  # pragma: no cover - mensagem amigável para execução local
    raise SystemExit(
        "Dependência ausente. Instale com: "
        "python -m pip install -r requirements-ci.txt"
    ) from exc


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SUPPORTED_ENVIRONMENTS = ("desenv", "main")


@dataclass(frozen=True)
class CatalogPlan:
    """Plano validado para uma execução contra um catálogo."""

    environment: str
    catalog: str
    description: str
    schemas: tuple[dict[str, str], ...]
    volumes: tuple[dict[str, Any], ...]


def parse_args() -> argparse.Namespace:
    """Lê argumentos de linha de comando e permite uso em CI/CD."""

    parser = argparse.ArgumentParser(
        description="Cria ou recria a estrutura de catálogos do HandsOn Beta."
    )
    parser.add_argument(
        "--config",
        default="config/catalogs.json",
        help="Caminho do manifesto JSON de catálogos.",
    )
    parser.add_argument(
        "--environment",
        choices=SUPPORTED_ENVIRONMENTS,
        default=os.getenv("DATABRICKS_ENVIRONMENT", "main"),
        help="Ambiente alvo. Default: DATABRICKS_ENVIRONMENT ou main.",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Remove o catálogo com CASCADE e recria a estrutura. Somente desenv.",
    )
    parser.add_argument(
        "--allow-destructive",
        action="store_true",
        help="Confirma que a execução destrutiva foi autorizada pelo pipeline.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Dispensa a confirmação interativa para --recreate.",
    )
    parser.add_argument(
        "--create-volumes",
        action="store_true",
        help="Cria os volumes declarados no manifesto.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Exibe os comandos SQL sem se conectar ou executá-los.",
    )
    return parser.parse_args()


def load_manifest(path: str | Path) -> dict[str, Any]:
    """Carrega e valida o manifesto JSON."""

    manifest_path = Path(path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifesto não encontrado: {manifest_path}")

    with manifest_path.open("r", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    if not isinstance(manifest, dict):
        raise ValueError("O manifesto deve conter um objeto JSON na raiz.")
    if not isinstance(manifest.get("environments"), dict):
        raise ValueError("O manifesto deve conter a chave 'environments'.")
    if not isinstance(manifest.get("volumes", []), list):
        raise ValueError("A chave 'volumes' deve conter uma lista.")
    return manifest


def validate_identifier(value: str, label: str) -> str:
    """Impede que valores de configuração sejam interpretados como SQL arbitrário."""

    if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} inválido: {value!r}. Use apenas letras, números e underscore, "
            "começando por letra ou underscore."
        )
    return value


def quote_identifier(value: str, label: str) -> str:
    """Valida e delimita um identificador de catálogo, schema ou volume."""

    return f"`{validate_identifier(value, label).replace('`', '``')}`"


def sql_string(value: str, label: str) -> str:
    """Converte texto de configuração em literal SQL seguro."""

    if not isinstance(value, str):
        raise ValueError(f"{label} deve ser texto.")
    return "'" + value.replace("'", "''") + "'"


def build_plan(manifest: dict[str, Any], environment: str) -> CatalogPlan:
    """Seleciona e valida a configuração do ambiente solicitado."""

    environments = manifest["environments"]
    if environment not in environments:
        available = ", ".join(sorted(environments))
        raise ValueError(
            f"Ambiente {environment!r} não está no manifesto. Disponíveis: {available}."
        )

    environment_config = environments[environment]
    if not isinstance(environment_config, dict):
        raise ValueError(f"Configuração inválida para o ambiente {environment!r}.")

    catalog = validate_identifier(environment_config.get("catalog"), "catalog")
    description = environment_config.get("description", "")
    if not isinstance(description, str):
        raise ValueError("A descrição do catálogo deve ser texto.")

    raw_schemas = environment_config.get("schemas", [])
    if not isinstance(raw_schemas, list) or not raw_schemas:
        raise ValueError("O ambiente deve declarar ao menos um schema.")

    schemas: list[dict[str, str]] = []
    for index, schema in enumerate(raw_schemas):
        if not isinstance(schema, dict):
            raise ValueError(f"Schema na posição {index} deve ser um objeto.")
        schema_name = validate_identifier(schema.get("name"), f"schema[{index}].name")
        comment = schema.get("comment", "")
        if not isinstance(comment, str):
            raise ValueError(f"Comentário do schema {schema_name!r} deve ser texto.")
        schemas.append({"name": schema_name, "comment": comment})

    raw_volumes = manifest.get("volumes", [])
    volumes: list[dict[str, Any]] = []
    for index, volume in enumerate(raw_volumes):
        if not isinstance(volume, dict):
            raise ValueError(f"Volume na posição {index} deve ser um objeto.")
        volume_schema = validate_identifier(
            volume.get("schema"), f"volume[{index}].schema"
        )
        volume_name = validate_identifier(volume.get("name"), f"volume[{index}].name")
        volume_type = volume.get("type", "managed")
        if volume_type not in {"managed", "external"}:
            raise ValueError(
                f"Tipo inválido para o volume {volume_name!r}: {volume_type!r}."
            )
        if volume_type == "external" and not volume.get("location"):
            raise ValueError(
                f"Volume externo {volume_name!r} deve declarar 'location'."
            )
        volumes.append(
            {
                "schema": volume_schema,
                "name": volume_name,
                "type": volume_type,
                "location": volume.get("location"),
                "comment": volume.get("comment", ""),
            }
        )

    return CatalogPlan(
        environment=environment,
        catalog=catalog,
        description=description,
        schemas=tuple(schemas),
        volumes=tuple(volumes),
    )


def build_statements(
    plan: CatalogPlan,
    *,
    recreate: bool = False,
    create_volumes: bool = False,
) -> list[str]:
    """Gera a sequência determinística de comandos SQL."""

    catalog_sql = quote_identifier(plan.catalog, "catalog")
    statements: list[str] = []

    if recreate:
        statements.append(f"DROP CATALOG IF EXISTS {catalog_sql} CASCADE")

    statements.append(
        "CREATE CATALOG IF NOT EXISTS "
        f"{catalog_sql} COMMENT {sql_string(plan.description, 'catalog.description')}"
    )

    for schema in plan.schemas:
        schema_sql = quote_identifier(schema["name"], "schema.name")
        statements.append(
            "CREATE SCHEMA IF NOT EXISTS "
            f"{catalog_sql}.{schema_sql} "
            f"COMMENT {sql_string(schema['comment'], 'schema.comment')}"
        )

    if create_volumes:
        for volume in plan.volumes:
            schema_sql = quote_identifier(volume["schema"], "volume.schema")
            volume_sql = quote_identifier(volume["name"], "volume.name")
            qualified_volume = f"{catalog_sql}.{schema_sql}.{volume_sql}"
            volume_comment = sql_string(volume["comment"], "volume.comment")
            if volume["type"] == "external":
                location = sql_string(volume["location"], "volume.location")
                statements.append(
                    "CREATE EXTERNAL VOLUME IF NOT EXISTS "
                    f"{qualified_volume} LOCATION {location} COMMENT {volume_comment}"
                )
            else:
                statements.append(
                    "CREATE VOLUME IF NOT EXISTS "
                    f"{qualified_volume} COMMENT {volume_comment}"
                )

    return [statement + ";" for statement in statements]


def require_destructive_confirmation(args: argparse.Namespace) -> None:
    """Aplica as barreiras de segurança para DROP CATALOG ... CASCADE."""

    if not args.recreate:
        return
    if args.environment != "desenv":
        raise ValueError(
            "--recreate só é permitido no ambiente desenv. "
            "Use a operação idempotente no ambiente main."
        )
    if not args.allow_destructive:
        raise ValueError(
            "--recreate exige --allow-destructive para evitar remoções acidentais."
        )
    if args.yes:
        return

    confirmation = input(
        "Digite RECREATE para remover e recriar o catálogo de desenvolvimento: "
    ).strip()
    if confirmation != "RECREATE":
        raise ValueError("Confirmação não recebida; operação destrutiva cancelada.")


def execute_statements(statements: Iterable[str]) -> None:
    """Executa os comandos em um SQL Warehouse usando credenciais do ambiente."""

    required = {
        "DATABRICKS_SERVER_HOSTNAME": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        "DATABRICKS_HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH"),
        "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise EnvironmentError(
            "Variáveis de conexão ausentes: " + ", ".join(missing)
        )

    with sql.connect(
        server_hostname=required["DATABRICKS_SERVER_HOSTNAME"],
        http_path=required["DATABRICKS_HTTP_PATH"],
        access_token=required["DATABRICKS_TOKEN"],
    ) as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                print(f"Executando: {statement}")
                cursor.execute(statement)


def main() -> int:
    """Ponto de entrada principal."""

    args = parse_args()
    try:
        manifest = load_manifest(args.config)
        plan = build_plan(manifest, args.environment)
        require_destructive_confirmation(args)
        statements = build_statements(
            plan,
            recreate=args.recreate,
            create_volumes=args.create_volumes,
        )

        print(
            f"Ambiente: {plan.environment} | Catálogo: {plan.catalog} | "
            f"Schemas: {len(plan.schemas)} | Volumes: "
            f"{len(plan.volumes) if args.create_volumes else 0}"
        )
        if args.dry_run:
            print("Modo dry-run: nenhum comando será enviado ao Databricks.")
            print("\n".join(statements))
            return 0

        execute_statements(statements)
        print("Estrutura de catálogos aplicada com sucesso.")
        return 0
    except (EnvironmentError, FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - falhas do serviço externo
        print(f"Falha na execução contra o Databricks: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
