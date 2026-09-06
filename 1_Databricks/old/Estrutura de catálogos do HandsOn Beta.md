# Estrutura de catálogos do HandsOn Beta

**Autor:** Fábio Fumio Wada  
**Objetivo:** provisionar a estrutura inicial de catálogos, schemas e volumes do projeto de previsão de bandeiras tarifárias no Databricks.

## 1. Visão geral

O repositório mantém a estrutura de dados como código. O manifesto [`config/catalogs.json`](../config/catalogs.json) descreve os ambientes, enquanto [`scripts/databricks_catalogs.py`](../scripts/databricks_catalogs.py) transforma essa declaração em comandos DDL executados em um SQL Warehouse do Databricks.

A operação padrão é **idempotente**: o script usa `IF NOT EXISTS`, portanto uma nova execução cria apenas os objetos ausentes. A documentação oficial do Databricks confirma esse comportamento para catálogos, schemas e volumes [1] [2] [3].

> O modo de recriação completa não é o comportamento padrão. Ele executa `DROP CATALOG ... CASCADE`, operação que remove recursivamente schemas e objetos filhos; por isso, permanece restrito ao ambiente `desenv` e requer uma autorização explícita [4].

## 2. Estrutura observada e estrutura gerenciada

A inspeção do workspace identificou o catálogo principal `handson_beta`, além dos catálogos `workspace`, `system`, `old_handson` e `samples`. O script gerencia somente a estrutura do projeto HandsOn Beta; os demais catálogos não são alterados.

O catálogo principal observado contém os schemas `default`, `landing_zone`, `bronze`, `prata`, `ouro` e `information_schema`. O schema `information_schema` é mantido fora do manifesto porque é gerenciado pelo próprio Unity Catalog. Os cinco schemas de negócio são declarados no manifesto para que a estrutura possa ser recriada de forma controlada.

| Camada | Objeto | Finalidade | Ambiente Desenv | Ambiente Main |
|---|---|---|---|---|
| Compatibilidade | `default` | Objetos auxiliares e compatibilidade com notebooks existentes | `handson_beta_desenv.default` | `handson_beta.default` |
| Entrada | `landing_zone` | Recepção dos arquivos brutos das fontes ANEEL, CCEE, ONS e IBGE | `handson_beta_desenv.landing_zone` | `handson_beta.landing_zone` |
| Medalhão Bronze | `bronze` | Persistência dos dados brutos com padronização mínima | `handson_beta_desenv.bronze` | `handson_beta.bronze` |
| Medalhão Prata | `prata` | Dados tratados, conformados e validados | `handson_beta_desenv.prata` | `handson_beta.prata` |
| Medalhão Ouro | `ouro` | Dados analíticos, features e produtos para o modelo | `handson_beta_desenv.ouro` | `handson_beta.ouro` |
| Sistema | `information_schema` | Metadados do Unity Catalog | Não gerenciado pelo script | Não gerenciado pelo script |

A separação lógica utiliza `handson_beta_desenv` para o branch `desenv` e `handson_beta` para o branch `main`. Caso o workspace utilize outro padrão de nomes, basta ajustar os valores no manifesto; o código não precisa ser duplicado.

## 3. Volumes opcionais

A interface do workspace indicou os volumes `handson_beta.landing_zone.arquivos` e `handson_beta.prata.bronze`. Eles foram declarados no manifesto, mas **não são criados por padrão**. Para criá-los, informe `--create-volumes` na execução manual ou habilite a entrada `create_volumes` no disparo manual do workflow.

A escolha é deliberada: um volume externo exige uma localização de armazenamento e privilégios adicionais. A documentação oficial distingue volumes gerenciados e externos, e exige `USE CATALOG`, `USE SCHEMA` e `CREATE VOLUME`; para volumes externos, também exige `CREATE EXTERNAL VOLUME` na external location [3].

## 4. Execução local

Instale as dependências do conector SQL:

```bash
python -m pip install -r requirements-ci.txt
```

Para validar a geração dos comandos sem tocar no Databricks:

```bash
python scripts/databricks_catalogs.py --environment desenv --dry-run
python scripts/databricks_catalogs.py --environment main --dry-run
```

Para aplicar a estrutura, configure as três variáveis de ambiente abaixo. O token não deve ser salvo no repositório:

```bash
export DATABRICKS_SERVER_HOSTNAME="<hostname-do-sql-warehouse>"
export DATABRICKS_HTTP_PATH="<http-path-do-sql-warehouse>"
export DATABRICKS_TOKEN="<token-ou-segredo-do-runner>"
python scripts/databricks_catalogs.py --environment desenv
```

Para criar também os volumes gerenciados declarados:

```bash
python scripts/databricks_catalogs.py --environment desenv --create-volumes
```

Para recriar o catálogo de desenvolvimento, a execução deve ser intencional e explícita:

```bash
python scripts/databricks_catalogs.py \
  --environment desenv \
  --recreate \
  --allow-destructive
```

O comando não aceita `--recreate` para `main`. Em execução não interativa, a confirmação pode ser fornecida com `--yes`, mas isso só deve ser usado em um job controlado de desenvolvimento:

```bash
python scripts/databricks_catalogs.py \
  --environment desenv \
  --recreate \
  --allow-destructive \
  --yes
```

## 5. Fluxo CI/CD

O workflow [`catalog-ci-cd.yml`](../.github/workflows/catalog-ci-cd.yml) implementa o seguinte fluxo:

| Evento | Validação | Deploy |
|---|---|---|
| Pull Request para `desenv` ou `main` | Compila o Python e executa dry-run para os dois ambientes | Não executa DDL |
| Push em `desenv` | Executa as validações e aplica `handson_beta_desenv` | Sim |
| Push em `main` | Executa as validações e aplica `handson_beta` | Sim |
| Execução manual | Permite escolher ambiente e criação de volumes | Sim, com aprovação do Environment do GitHub |

A documentação do Databricks recomenda Declarative Automation Bundles para descrever recursos como arquivos-fonte versionados e permitir que ferramentas externas, como GitHub Actions, disparem implantações [5]. Este primeiro script mantém o DDL de catálogos simples e transparente; o mesmo manifesto pode ser incorporado posteriormente a um bundle maior com jobs, notebooks, pipelines e modelos.

### Segredos necessários no GitHub

Cadastre os seguintes segredos no **Environment** `desenv` e no **Environment** `main`. O valor pode ser diferente por ambiente, permitindo que cada branch use seu próprio SQL Warehouse e credencial.

| Segredo | Conteúdo |
|---|---|
| `DATABRICKS_SERVER_HOSTNAME` | Hostname do SQL Warehouse, sem `https://` |
| `DATABRICKS_HTTP_PATH` | HTTP Path da aba de conexão do SQL Warehouse |
| `DATABRICKS_TOKEN` | Token ou credencial disponibilizada ao runner do GitHub |

O workflow usa apenas `contents: read`, não imprime os segredos e não os grava no repositório. Recomenda-se configurar aprovação obrigatória no Environment `main` antes da primeira implantação em produção.

## 6. Sincronização GitHub–Databricks

O GitHub é a fonte de verdade para o manifesto e o código de provisionamento. O Databricks recebe as alterações por meio do workflow após a revisão e o merge no branch correspondente. A ordem recomendada é: alterar manifesto ou script no branch `desenv`, abrir Pull Request, validar o dry-run, aprovar e fazer merge, observar a execução contra `handson_beta_desenv`, e somente depois promover a mesma alteração para `main`.

Não se deve alterar manualmente a estrutura de catálogos em produção sem registrar a mudança no repositório. Se uma alteração manual for necessária em uma emergência, o manifesto deve ser atualizado imediatamente para que GitHub e Databricks voltem a representar o mesmo estado desejado.

## 7. Limitações e próximos passos

O script cria somente o contêiner lógico do projeto: catálogo, schemas e volumes opcionais. Ele não cria tabelas, views, grants, external locations, storage credentials ou jobs de ingestão. Esses recursos devem ser adicionados em etapas posteriores, com seus próprios manifestos e testes.

Também não é seguro inferir a localização física dos volumes a partir do nome exibido no Catalog Explorer. Para volumes externos, inclua `"type": "external"` e uma `"location"` aprovada pelo administrador do Unity Catalog no manifesto, além de garantir o privilégio correspondente.

## Referências

[1]: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-catalog "Databricks — CREATE CATALOG"

[2]: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-schema "Databricks — CREATE SCHEMA"

[3]: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-volume "Databricks — CREATE VOLUME"

[4]: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-drop-catalog "Databricks — DROP CATALOG"

[5]: https://docs.databricks.com/aws/en/dev-tools/ci-cd/ "Databricks — CI/CD on Databricks"
