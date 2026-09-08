# Diagnóstico definitivo das dependências Excel

## Por que a mensagem continua

O instalador anterior executa `pip` no ambiente Python efêmero do processo que chamou o arquivo. Isso não garante que `xlrd` e `openpyxl` estejam disponíveis no ambiente efetivo da Python file task ou nos executors Spark. A mensagem persistente indica que o processo que lê o arquivo Excel não encontra o engine no seu próprio `sys.path`.

A documentação do Databricks diferencia bibliotecas de notebook, que não persistem entre sessões, de bibliotecas de Compute, que ficam disponíveis para notebooks e Jobs executados no Compute [1] [2]. Por isso, a instalação persistente deve ser configurada no Compute ou na definição do Job, e não somente dentro de um Python file.

## Arquivo de requisitos

Use o arquivo `requirements_excel.txt` como dependência do Job ou como arquivo de requisitos de uma biblioteca do Compute:

```text
pandas>=2.2,<3
xlrd>=2.0.1
openpyxl>=3.1.0
```

A restrição `pandas<3` preserva a compatibilidade observada com `databricks-connect 18.0.9`.

## Procedimento no Compute

Abra **Compute**, selecione o Compute utilizado pela Python file task, acesse a aba **Libraries**, escolha **Install New**, selecione **PyPI** e adicione cada pacote com a respectiva versão. Como alternativa, use `requirements_excel.txt` quando o tipo de biblioteca e o modo de acesso do Compute permitirem. Reinicie o Compute após a instalação e abra uma nova sessão.

As bibliotecas instaladas no Compute ficam disponíveis para os notebooks e Jobs que executam nesse Compute [1]. A documentação também informa que uma sessão já aberta não passa a enxergar imediatamente uma nova biblioteca; é necessário iniciar uma nova sessão [1].

## Procedimento no Job

Na configuração do Job, adicione bibliotecas PyPI à tarefa ou ao Job com os pacotes `pandas>=2.2,<3`, `xlrd>=2.0.1` e `openpyxl>=3.1.0`. Se a tarefa utilizar um Job Compute novo, configure as bibliotecas no próprio Job Compute. Reinicie o Compute ou execute uma nova instância depois de alterar as bibliotecas.

## Diagnóstico

Execute `scripts/00_verificar_dependencias_excel.py` como uma Python file task antes das ingestões. O script informa o interpretador efetivo, `sys.path`, versão e origem de `pandas`, `xlrd` e `openpyxl` no driver. Quando `sparkContext` está disponível, ele também realiza uma verificação em um executor.

A saída esperada precisa mostrar uma versão e uma origem para `xlrd` e `openpyxl` tanto no driver quanto no executor. Se um deles aparecer como `indisponivel`, instale a dependência no Compute ou Job correto, reinicie e execute o diagnóstico novamente.

## Ordem recomendada

Execute primeiro `00_verificar_dependencias_excel.py`. Depois, execute `01_ingestao_bronze_aneel.py` e `02_ingestao_bronze_ibge.py`. O arquivo `00_instalar_dependencias_excel.py` pode ser usado apenas como tentativa de instalação no ambiente do processo, mas não substitui a configuração persistente de bibliotecas do Compute ou Job.

## Referências

[1]: https://docs.databricks.com/aws/en/libraries/cluster-libraries "Compute-scoped libraries — Databricks"
[2]: https://docs.databricks.com/aws/en/libraries/notebooks-python-libraries "Notebook-scoped Python libraries — Databricks"
[3]: https://docs.databricks.com/aws/en/libraries/ "Install libraries — Databricks"
