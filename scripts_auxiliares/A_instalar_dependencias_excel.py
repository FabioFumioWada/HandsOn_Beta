"""Instala e valida as dependências Excel usadas pelos fluxos Bronze.

Dependências instaladas:
    pandas>=2.2,<3       # compatível com databricks-connect 18.x
    xlrd>=2.0.1          # leitura de arquivos .xls
    openpyxl>=3.1.0     # leitura de arquivos .xlsx

Execute este arquivo como Python file no driver do Databricks. Para Jobs
multi-node ou clusters compartilhados, a forma recomendada é cadastrar as
mesmas dependências como bibliotecas PyPI do compute/Job, pois uma instalação
via subprocess atua no ambiente Python do processo que está executando o script.
"""

from __future__ import annotations

import importlib
import importlib.metadata as metadata
import subprocess
import sys
from typing import Dict, Tuple


PACKAGES = (
    "pandas>=2.2,<3",
    "xlrd>=2.0.1",
    "openpyxl>=3.1.0",
)

MODULES = {
    "pandas": "pandas",
    "xlrd": "xlrd",
    "openpyxl": "openpyxl",
}


def install_packages() -> None:
    """Instala as dependências usando o mesmo interpretador do processo."""
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        *PACKAGES,
    ]
    print("Executando instalação das dependências:")
    print(" ".join(command))
    subprocess.check_call(command)


def installed_versions() -> Dict[str, str]:
    """Retorna as versões instaladas dos pacotes solicitados."""
    versions: Dict[str, str] = {}
    for distribution in MODULES:
        versions[distribution] = metadata.version(distribution)
    return versions


def validate_imports() -> Tuple[bool, Dict[str, str]]:
    """Importa os módulos e confirma que as versões estão acessíveis."""
    versions = installed_versions()
    for module_name in MODULES.values():
        importlib.import_module(module_name)
    return True, versions


def main() -> None:
    print(f"Interpretador Python: {sys.executable}")
    install_packages()
    importlib.invalidate_caches()
    success, versions = validate_imports()

    if not success:
        raise RuntimeError("As dependências foram instaladas, mas não puderam ser validadas.")

    print("Dependências instaladas e importadas com sucesso:")
    for package, version in versions.items():
        print(f"- {package}: {version}")

    print(
        "Aviso: em clusters multi-node ou Jobs recorrentes, prefira cadastrar "
        "pandas>=2.2,<3, xlrd>=2.0.1 e openpyxl>=3.1.0 como bibliotecas PyPI do compute/Job "
        "e reiniciar o compute antes de executar a ingestão Bronze."
    )


if __name__ == "__main__":
    main()
