import subprocess
import sys


def instalar_bibliotecas():
  # Lista de bibliotecas com as versões mínimas exigidas
  bibliotecas = ["pandas>=2.2", "xlrd>=2.0.1", "openpyxl>=3.1.0"]

  print("Iniciando a instalação das bibliotecas...")

  for lib in bibliotecas:
    try:
      print(f"-> Instalando {lib}...")
      subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
    except subprocess.CalledProcessError as e:
      print(f"Erro ao instalar o pacote {lib}: {e}")

  print("\nProcesso de instalação concluído!")


if __name__ == "__main__":
  instalar_bibliotecas()