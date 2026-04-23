# Sell-In Data Pipeline com MinIO + Airflow + Grafana

Stack pronta em `docker-compose` para demonstrar **testes automatizados em pipelines de dados** com foco em qualidade, validação de contrato e visualização dos **PDVs que mais vendem por semana**.

## O que essa solução entrega

- **MinIO** como camada de ingestão em estilo data lake
- **Airflow** para orquestrar a pipeline ETL
- **PostgreSQL** como camada analítica simples para o Grafana
- **Grafana** com dashboard provisionado automaticamente
- **Testes automatizados** Código validado usando `pytest`, `hypothesis` e mocks/fakes.
- **Validação de schema/contrato** do CSV antes da transformação
- **Carga automática do CSV de exemplo** no bucket `raw`

## Regra de negócio implementada

A pipeline lê o CSV de sell-in da pasta `raw`, valida o contrato do arquivo e gera uma tabela analítica com:

- `anio_semana`
- `id_pdv`
- `venda_total`
- `quantidade_total`
- `preco_medio`
- `sku_distintos`
- `ranking_semana`

Depois disso, o Grafana exibe:

- Media de vendas por dia
- Total vendido
- Semana com menor venda
- Semana com maior venda
- Ultimos processamentes e status
- Ranking agrupado de PDVs e SKUs

## Arquitetura

```text
CSV -> MinIO/raw -> Airflow DAG -> validação -> transformação semanal -> PostgreSQL -> Grafana
                                      \
                                       -> MinIO/processed
                                      \
                                       -> MinIO/erro (quando a validação falha)
```

## Estrutura do projeto

```text
sellin_observability_stack/
├── airflow/
├── dags/
├── data/
│   ├── raw/
│   └── mocks/
├── grafana/
├── pipeline/
├── sql/
├── tests/
├── docker-compose.yml
└── README.md
```

## Como subir

```bash
cp .env.example .env
docker compose up -d --build
```
## Como destruir
```bash
docker compose down -v
```
## Portas

- MinIO API: `9000`
- MinIO Console: `9001`
- Airflow: `8080`
- Grafana: `3000`
- PostgreSQL: `5432`

## Credenciais padrão

### MinIO
- user: `minioadmin`
- password: `minioadmin123`

### Airflow
- user: `airflow`
- password: `airflow`

### Grafana
- user: `admin`
- password: `admin`

## Buckets criados automaticamente

- `raw`
- `processed`
- `erro`

O arquivo `qro_sellin_filtered_5000.csv` já sobe automaticamente para `raw` no bootstrap inicial.

## Para subir um CSV valido
- Acesse no seu navegador http://localhost:9001 e insira usuário e senha
- Entre no bucket `raw`, clique em `upload``
- Navegue ate a pasta data/mocks e suba o arquivo `qro_sellin_validado_para_pipeline.csv`. E aguarde o airflow rodar automaticamente.
- Acompanhe no painel do airflow, assim que ele rodar visualize dos dados no painel do grafana
## DAG

A DAG `sellin_minio_to_grafana` roda a cada 2 minutos e faz:

1. procura o CSV mais recente no bucket `raw`
2. valida o contrato do arquivo
3. transforma os dados
4. em caso de falha de qualidade, move o arquivo para o bucket `erro` e registra o evento em `pipeline_runs`
5. em caso de sucesso, grava o resultado na tabela `weekly_pdv_sales` e `sku_pdv_sales`
6. salva um CSV tratado no bucket `processed`

## Executando os testes

### Local com Python

```bash
pip install -r airflow/requirements.txt
pytest -vv tests
```

### Dentro do container do Airflow

```bash
docker compose exec airflow-webserver pytest -vv /opt/airflow/project/tests
```

## Tipos de testes cobertos

### 1. Testes de contrato / schema
Validação das colunas obrigatórias, formato de datas, inteiros e decimais.

### 2. Testes unitários
Validação das funções de transformação semanal.

### 3. Testes com dados sintéticos
Uso de `hypothesis` para garantir propriedades da transformação.

### 4. Testes com mocks/fakes
Aproveita os arquivos de mock já existentes em `data/mocks`.

## Subindo um novo CSV para o MinIO

```bash
python scripts/upload_to_minio.py data/raw/qro_sellin_filtered_5000.csv
```

## Query útil para validar no Postgres

```sql
SELECT anio_semana, ranking_semana, id_pdv, venda_total
FROM weekly_pdv_sales
ORDER BY anio_semana DESC, ranking_semana ASC
LIMIT 20;
```

## Observações importantes

- Para ambiente de demonstração, o PostgreSQL foi usado como camada analítica simples.
- Em produção, você poderia trocar por ClickHouse, BigQuery, Trino, Athena ou outro motor analítico.
- Para trigger real por evento no MinIO, uma evolução natural seria configurar **bucket notifications** para webhook, GCS com Eventarc, Kafka, RabbitMQ ou PubSub, e o Airflow reagir via API ou sensor mais específico.

## Próximas evoluções recomendadas

- adicionar testes de integração com banco efêmero
- expor métricas técnicas da DAG no Prometheus


## Fluxo de falha controlada e observabilidade do pipeline

Quando a validação encontra uma inconsistência de regra de negócio, a DAG falha de forma intencional. Esse comportamento faz parte da demonstração de qualidade de dados e evita que um arquivo inválido alimente o dashboard analítico.

Fluxo implementado:

1. o Airflow detecta o CSV mais recente no bucket `raw`
2. a validação do contrato roda antes da publicação analítica
3. se houver erro, a DAG falha propositalmente
4. o arquivo ofensivo é movido de `raw` para `erro`
5. a execução é registrada na tabela `pipeline_runs`
6. o Grafana exibe uma tabela com os últimos processamentos, incluindo status de sucesso ou erro, linha inválida, regra violada e motivo da falha
7. após corrigir o arquivo de origem e reenviá-lo, a pipeline volta a processar com sucesso, grava a tabela analítica `weekly_pdv_sales` e atualiza o dashboard principal

Esse fluxo permite contar a história completa na apresentação: falha controlada, rastreabilidade do erro, proteção do consumo analítico e reprocessamento com sucesso.
