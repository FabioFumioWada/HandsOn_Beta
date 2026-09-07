# Instalação das dependências Excel no Databricks

O script `scripts/00_instalar_dependencias_excel.py` instala e valida as bibliotecas necessárias para os fluxos Bronze ANEEL e IBGE:

| Pacote | Uso |
|---|---|
| `pandas` | Leitura tabular e suporte à leitura dos arquivos Excel. |
| `xlrd>=2.0.1` | Leitura de arquivos `.xls`. |
| `openpyxl>=3.1.0` | Leitura de arquivos `.xlsx`. |

O arquivo deve ser executado como **Python file** no Databricks. Ele usa o mesmo interpretador Python da execução para chamar `pip`, instalar ou atualizar os pacotes, importar os módulos e imprimir as versões instaladas.

A forma mais estável para clusters compartilhados, multi-node ou Jobs recorrentes é cadastrar os três pacotes como bibliotecas PyPI do compute ou nas dependências do Job:

```text
pandas
xlrd>=2.0.1
openpyxl>=3.1.0
```

Depois da instalação, reinicie o compute. Não misture `%pip install` dentro dos scripts de ingestão, pois eles são Python files e não notebooks. O instalador é um script auxiliar separado e deve ser executado antes de `01_ingestao_bronze_aneel.py` e `02_ingestao_bronze_ibge.py`.

A execução deve terminar exibindo a versão de cada pacote. Se ocorrer erro de permissão durante `pip install`, use a instalação como biblioteca PyPI do compute ou Job, que é a opção recomendada para o ambiente Databricks.
