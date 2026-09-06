# Ingestão de arquivos públicos para a landing zone

**Projeto:** HandsOn Beta  
**Responsável pelo artefato:** Fábio Fumio Wada  
**Script:** [`scripts/baixar_arquivos_portais.py`](../scripts/baixar_arquivos_portais.py)  
**Versão:** 1.0.0  
**Data de referência das fontes:** 06/09/2026

## Objetivo

O processo descobre e baixa, de forma idempotente, os recursos públicos dos conjuntos selecionados de ANEEL e ONS, todos os conjuntos públicos do catálogo CCEE e os arquivos do diretório oficial de Estimativas de População do IBGE. Os arquivos são gravados na Unity Catalog Volume indicada pelo projeto:

```text
/Volumes/handson_beta/landing_zone/arquivos/
```

O nome local preserva o nome do arquivo recebido do catálogo ou da URL e adiciona o prefixo da plataforma. Por exemplo, `bandeira-tarifaria-adicional.csv` torna-se `ANEEL-bandeira-tarifaria-adicional.csv`.

> O script não mantém uma lista fixa de anos ou arquivos. A cada execução ele consulta os catálogos públicos, registra o inventário descoberto e baixa somente o que é novo ou foi alterado.

## Fontes e mecanismos de descoberta

| Plataforma | Escopo implementado | Mecanismo de descoberta | Observação |
| --- | --- | --- | --- |
| ANEEL | Conjunto `bandeiras-tarifarias` | API CKAN `package_show` | A página do conjunto lista dicionários PDF e dados CSV, com atualização mensal [1]. |
| ONS | Conjunto `ena-diario-por-reservatorio` | API CKAN `package_show` | O catálogo publica anos e formatos conforme disponibilidade; os dados podem ser revisados após publicação [2]. |
| IBGE | Diretório `Estimativas_de_Populacao` | Índices HTML do FTP oficial, com tentativa complementar da página institucional | A página institucional pode bloquear clientes automatizados; por isso o FTP oficial é o mecanismo principal [3]. |
| CCEE | Todos os datasets públicos | API CKAN `package_search`, paginada | A API observada retornou 204 conjuntos na consulta de referência; a quantidade deve ser redescoberta em cada execução [4]. |

A consulta de metadados não executa instruções remotas nem baixa arquivos arbitrários fora dos domínios retornados pelos catálogos. O conteúdo dos arquivos é tratado como dado bruto para a camada **Landing/Bronze**.

## Arquitetura medalhão

O downloader é a etapa de ingestão da arquitetura medalhão. Ele não transforma os dados, pois a preservação do bruto é necessária para rastreabilidade e reprocessamento.

| Camada | Responsabilidade | Artefatos esperados |
| --- | --- | --- |
| Landing / Bronze | Preservar o arquivo original baixado, com prefixo da plataforma e sem alteração de conteúdo | Arquivos `ANEEL-*`, `ONS-*`, `IBGE-*` e `CCEE-*` na Volume |
| Silver | Padronizar formatos, codificação, nomes de colunas, tipos, datas e chaves; deduplicar usando o manifesto e o hash SHA-256 | Tabelas Delta por fonte e domínio, a serem implementadas em notebook/job posterior |
| Gold | Consolidar variáveis para estudo e previsão das bandeiras tarifárias, com métricas e features prontas para ML | Tabelas Delta de features, treino, validação e previsões, a serem implementadas em etapa posterior |

O diretório `_controle` é operacional e não faz parte dos dados analíticos. Ele guarda o catálogo descoberto, o manifesto de downloads, o resumo da execução e logs JSON Lines.

## Controle de ritmo e proteção contra bloqueio

O script processa os recursos sequencialmente. Antes de cada requisição, aplica uma espera mínima (`--intervalo-segundos`, padrão de 5 segundos) e um atraso aleatório adicional (`--jitter-segundos`, padrão de 2 segundos). Falhas transitórias recebem retry com backoff exponencial, jitter e respeito ao cabeçalho `Retry-After` quando fornecido. Códigos permanentes, como 403, não são repetidos indefinidamente.

Esse mecanismo é deliberadamente conservador. O parâmetro controla a frequência de chamadas aos portais; ele não deve ser substituído por múltiplas requisições paralelas. O loop de downloads evita criar uma rajada de conexões que possa ser interpretada como abuso pelos portais ou sobrecarregar o driver do Databricks.

## Idempotência e rastreabilidade

A execução usa quatro mecanismos complementares. Primeiro, o catálogo é redescoberto e salvo em `_controle/catalogo_descoberto.json`. Segundo, o manifesto `_controle/manifest.json` associa a URL, o recurso e a assinatura remota ao caminho local. Terceiro, arquivos são baixados para `_controle/_temporarios` e movidos por renomeação atômica somente depois de concluídos; um arquivo parcial não é apresentado como válido. Quarto, cada arquivo recebido recebe SHA-256 e tamanho em bytes.

Se o arquivo local existir e a assinatura remota não tiver mudado, o download é ignorado. Se o catálogo mudar hash, data de modificação, tamanho ou o arquivo local desaparecer, o recurso será baixado novamente. Para forçar reprocessamento, use `--forcar`.

## Execução no Databricks

Em um Databricks Repo, execute o script a partir de uma célula `%sh`, ajustando o caminho do workspace:

```bash
%sh
python /Workspace/Repos/<usuario>/HandsOn_Beta/scripts/baixar_arquivos_portais.py \
  --output-dir /Volumes/handson_beta/landing_zone/arquivos/ \
  --intervalo-segundos 5 \
  --jitter-segundos 2
```

Antes da execução, o principal ou usuário do job precisa ter permissão de gravação na Volume. Uma execução de teste que apenas descobre os arquivos pode ser feita com:

```bash
%sh
python /Workspace/Repos/<usuario>/HandsOn_Beta/scripts/baixar_arquivos_portais.py \
  --fontes ANEEL ONS IBGE CCEE \
  --dry-run \
  --output-dir /Volumes/handson_beta/landing_zone/arquivos/
```

Para uma execução incremental de uma única fonte:

```bash
%sh
python /Workspace/Repos/<usuario>/HandsOn_Beta/scripts/baixar_arquivos_portais.py \
  --fontes ONS \
  --intervalo-segundos 8 \
  --jitter-segundos 3
```

Os principais parâmetros são:

| Parâmetro | Padrão | Finalidade |
| --- | ---: | --- |
| `--fontes` | quatro fontes | Limitar a execução a uma ou mais plataformas |
| `--output-dir` | Volume do projeto | Alterar o destino quando necessário |
| `--intervalo-segundos` | 5 | Espera mínima entre requisições |
| `--jitter-segundos` | 2 | Atraso aleatório complementar |
| `--tentativas` | 5 | Número máximo de tentativas transitórias |
| `--pagina-ccee` | 50 | Tamanho da página na API CKAN da CCEE |
| `--max-datasets-ccee` | 0 | Limite de conjuntos CCEE; zero significa todos |
| `--forcar` | desligado | Rebaixar recursos já registrados como válidos |
| `--dry-run` | desligado | Descobrir e catalogar sem baixar |
| `--parar-no-erro` | desligado | Interromper no primeiro erro em vez de continuar |

## Agendamento e CI/CD

O código é determinístico e não requer uma sessão de IA a cada execução. Recomenda-se criar um Databricks Job periódico, por exemplo diário ou semanal conforme a frequência de atualização da fonte, e registrar o caminho da branch e do commit usado pelo job. A frequência do job deve ser independente do temporizador entre downloads: o primeiro controla atualização do dado e o segundo controla a taxa de requisições dentro de uma execução.

O fluxo mínimo de CI/CD deverá executar, na branch `desenv`, validação sintática e testes de descoberta sem download. Depois da revisão, a alteração pode ser promovida para `main` e o Job do Databricks deve apontar para um commit versionado da `main`. Uma sugestão de validação local é:

```bash
python -m py_compile scripts/baixar_arquivos_portais.py
python scripts/baixar_arquivos_portais.py --fontes ANEEL ONS --dry-run --output-dir /tmp/handson_beta_landing
```

O `--dry-run` ainda consulta os catálogos, portanto deve respeitar limites de rede e os parâmetros de intervalo. Em CI sem rede externa, utilize mocks HTTP em testes unitários ou execute apenas a compilação e as verificações estáticas.

## Tratamento de falhas

Uma fonte que falha na descoberta é registrada em `discovery_errors` e não impede, por padrão, a tentativa das demais fontes. Um arquivo que falha no download é registrado no log JSONL e no manifesto, e o processo continua para os recursos seguintes. O código de saída é `0` quando a execução termina sem erro, `2` quando houve erro de descoberta ou download e um código diferente de zero para erro de configuração fatal.

O operador deve revisar os seguintes arquivos após o job:

```text
/Volumes/handson_beta/landing_zone/arquivos/_controle/ultimo_resumo.json
/Volumes/handson_beta/landing_zone/arquivos/_controle/catalogo_descoberto.json
/Volumes/handson_beta/landing_zone/arquivos/_controle/manifest.json
/Volumes/handson_beta/landing_zone/arquivos/_controle/download_*.jsonl
```

## Referências

[1]: https://dadosabertos.aneel.gov.br/dataset/bandeiras-tarifarias "ANEEL — Bandeiras Tarifárias"

[2]: https://dados.ons.org.br/dataset/ena-diario-por-reservatorio "ONS — ENA Diário por Reservatório"

[3]: https://ftp.ibge.gov.br/Estimativas_de_Populacao/ "IBGE — diretório FTP de Estimativas de População"

[4]: https://dadosabertos.ccee.org.br/api/3/action/package_search?rows=5&start=0 "CCEE — API CKAN package_search"
