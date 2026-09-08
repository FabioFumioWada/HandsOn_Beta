
## Pesquisa inicial de fontes ONS — 2026-09-07

A fonte oficial é o Portal de Dados Abertos do ONS: https://dados.ons.org.br/. O portal informa que reúne dados históricos do setor elétrico brasileiro e lista conjuntos como Balanço de Energia nos Subsistemas, Carga de Energia Diária e Mensal, Dados Hidrológicos de Reservatórios, EAR Diário, ENA Diário, Geração por Usina, Intercâmbios e Reservatórios.

O conjunto validado para o primeiro contrato de ingestão é **ENA Diário por Reservatório**: https://dados.ons.org.br/dataset/ena-diario-por-reservatorio. O próprio portal descreve os dados como grandezas de Energia Natural Afluente com periodicidade diária por reservatório e informa que a ENA Bruta representa a energia produzível e a ENA Armazenável considera as vazões naturais descontadas das vazões vertidas. O portal também alerta que os dados passam por processo recorrente de consistência e podem ser atualizados após a publicação.

O dataset disponibiliza dicionário em PDF e JSON e recursos anuais em PARQUET, CSV e XLSX, incluindo 2026, 2025, 2024 e anos anteriores. Os CSVs são recursos oficiais do portal; a página do dicionário indica codificação UTF-8 e delimitador próprio do recurso. A estrutura recomendada para a Bronze é manter os arquivos na Landing Zone, listar todos os arquivos com prefixo ONS, importar CSV/PARQUET/XLSX elegíveis, agrupar por dataset e ano quando aplicável, preservar metadados de origem, gerar dicionário de dados e registrar tudo em `handson_beta.controle_global.controle_importacao`.

O downloader existente do projeto já contém a descoberta CKAN para ONS usando `https://dados.ons.org.br/api/3/action/package_show?id=ena-diario-por-reservatorio`, o que confirma que o primeiro dataset ONS pode ser alimentado pelo mesmo mecanismo de catálogo/manifesto usado no fluxo de arquivos públicos. A implementação Bronze deverá ainda tratar o caminho `dbfs:/Volumes/...` para Spark e `/Volumes/...` para leitores Python quando XLSX for utilizado.

Fontes consultadas:

1. Portal ONS Dados Abertos — https://dados.ons.org.br/
2. Dataset ONS ENA Diário por Reservatório — https://dados.ons.org.br/dataset/ena-diario-por-reservatorio
3. Repositório colaborativo informado pelo portal — https://github.com/ONSBR/DadosAbertos

## Contrato Bronze definido para a ONS

A primeira implementação ONS será baseada no conjunto `ena-diario-por-reservatorio`, mantendo espaço para inclusão de outros datasets do portal sem alterar o padrão. O dicionário oficial JSON identifica os seguintes campos de negócio:

| Campo | Significado | Unidade ou observação |
|---|---|---|
| `nom_reservatorio` | Nome do reservatório | Identificação textual |
| `cod_resplanejamento` | Código do reservatório nos modelos de planejamento | Código |
| `tip_reservatorio` | Tipo de reservatório | Categoria |
| `nom_bacia` | Nome da bacia | Dimensão geográfica |
| `nom_ree` | Nome do Reservatório Equivalente de Energia | Dimensão energética |
| `id_subsistema` | Código do subsistema | Código |
| `nom_subsistema` | Nome do subsistema | Dimensão operacional |
| `ena_data` | Dia observado da medida | Data |
| `ena_bruta_res_mwmed` | ENA bruta | MWmed |
| `ena_bruta_res_percentualmlt` | ENA bruta relativa à MLT | Percentual da MLT |
| `ena_armazenavel_res_mwmed` | ENA armazenável | MWmed |
| `ena_armazenavel_res_percentualmlt` | ENA armazenável relativa à MLT | Percentual da MLT |
| `ena_queda_bruta` | ENA por queda | MWmed |
| `mlt_ena` | Média de longo termo da ENA | MWmed |

A tabela de inventário será `handson_beta.bronze_ons.lista_arquivos_ons`, a tabela de dicionário será `handson_beta.bronze_ons.dicionario_dados_ons` e a tabela de dados será `handson_beta.bronze_ons.ons_ena_diario_reservatorios`. Os metadados `ano`, `arquivo_origem`, `caminho_origem`, `aba_origem`, `data_modificacao_origem`, `data_ingestao_utc`, `chave_duplicidade`, `indice_duplicidade`, `quantidade_duplicidade` e `registro_duplicado` seguem o mesmo princípio da ingestão IBGE.

Quando o catálogo oferecer PARQUET, CSV e XLSX para o mesmo ano, o ingestador selecionará somente um recurso por combinação de dataset e ano, priorizando PARQUET, depois CSV, depois XLSX e XLS. Essa regra evita que as representações do mesmo ano sejam duplicadas na tabela Bronze. Os recursos alternativos permanecem registrados no inventário e no controle global com status `NAO_SELECIONADO_FORMATO_CANONICO`.

Os CSVs ONS são tratados como UTF-8, delimitados por ponto e vírgula e com ponto como separador decimal, conforme a descrição oficial do dataset. Os arquivos Excel são lidos com `openpyxl` ou `xlrd` e com caminho POSIX `/Volumes/...`; as APIs Spark preservam o caminho lógico `dbfs:/Volumes/...`. Parquet é lido diretamente pelo Spark.

## Artefato implementado

Foi criado `scripts/05_ingestao_bronze_ons.py`. O arquivo passou por `python3 -m py_compile` e ainda não foi implantado nem executado no Databricks. A execução foi deliberadamente mantida separada do Job completo para permitir validação exclusiva da ONS.

## Referências

[1]: https://dados.ons.org.br/ "Portal de Dados Abertos do ONS"
[2]: https://dados.ons.org.br/dataset/ena-diario-por-reservatorio "Dataset ENA Diário por Reservatório"
[3]: https://dados.ons.org.br/api/3/action/package_show?id=ena-diario-por-reservatorio "API CKAN do dataset ENA Diário por Reservatório"
[4]: https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/ena_reservatorio_di/DicionarioDados_EnaPorReservatorio.json "Dicionário JSON oficial do dataset ONS"
[5]: https://github.com/ONSBR/DadosAbertos "Repositório colaborativo Dados Abertos ONS"
