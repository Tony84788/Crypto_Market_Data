# Cryptocurrency Market Data Platform

An end-to-end cryptocurrency market data engineering platform that extracts cryptocurrency market data from the CoinGecko API, streams events through Apache Kafka, stores raw and processed data in a local data lake, loads analytical data into Google BigQuery, transforms and tests warehouse data with dbt, and orchestrates the complete workflow with Apache Airflow.

The project is designed as a zero-cost learning and portfolio project using local infrastructure, Docker, Kafka, and the BigQuery Sandbox.

The current implementation provides a streaming-oriented architecture, while the complete Airflow pipeline is manually triggered during development.

---

## Architecture

```text
                         ┌─────────────────────┐
                         │    CoinGecko API    │
                         │                     │
                         │ Cryptocurrency      │
                         │ Market Data         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Python Extractor  │
                         │                     │
                         │ requests            │
                         │ validation          │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Apache Kafka      │
                         │                     │
                         │ crypto-market-data  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Kafka Consumer    │
                         │                     │
                         │ validation          │
                         │ processing          │
                         │ DLQ handling        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │       Local Data Lake        │
                    │                              │
                    │ RAW JSON                     │
                    │        ↓                     │
                    │ Processed Parquet            │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │        Google BigQuery       │
                    │                              │
                    │ crypto_prices                │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │             dbt              │
                    │                              │
                    │ staging                      │
                    │ deduplication                │
                    │ freshness                    │
                    │ tests                        │
                    │ marts                        │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │        Apache Airflow        │
                    │                              │
                    │ End-to-end orchestration     │
                    └──────────────────────────────┘
```

---

# 1. Project Overview

The Cryptocurrency Market Data Platform demonstrates an end-to-end modern data engineering workflow.

The platform performs the following operations:

1. Extract cryptocurrency market data from CoinGecko.
2. Publish cryptocurrency events to Apache Kafka.
3. Consume and validate Kafka events.
4. Route invalid events to a Dead Letter Queue.
5. Store valid raw events in a local data lake.
6. Transform raw events using Pandas.
7. Store processed data as Parquet.
8. Load processed Parquet data into Google BigQuery.
9. Transform and test warehouse data using dbt.
10. Create analytical marts.
11. Orchestrate the complete workflow using Apache Airflow.
12. Validate Python code, Terraform configuration, and dbt project structure through GitHub Actions CI.

The pipeline currently processes:

* Bitcoin
* Ethereum
* Solana

---

# 2. Data Source

## CoinGecko

The project uses CoinGecko as its external cryptocurrency market data provider.

API documentation:

https://docs.coingecko.com/

Primary API endpoint:

```text
https://api.coingecko.com/api/v3/coins/markets
```

The API provides market information such as:

* Cryptocurrency ID
* Symbol
* Name
* Current price
* Market capitalization
* Market rank
* Trading volume
* 24-hour high
* 24-hour low
* 24-hour price change
* 24-hour percentage change
* Circulating supply
* Total supply
* Maximum supply
* All-time high
* All-time low
* Last updated timestamp

Example cryptocurrency IDs:

```text
bitcoin
ethereum
solana
```

The Python client supports an optional CoinGecko API key through environment configuration.

---

# 3. Technology Stack

| Technology       | Purpose                                    |
| ---------------- | ------------------------------------------ |
| Python           | Data ingestion, validation, and processing |
| Requests         | CoinGecko API communication                |
| Pandas           | Data transformation                        |
| PyArrow          | Parquet processing                         |
| Apache Kafka     | Event streaming                            |
| Docker           | Kafka container infrastructure             |
| Local Data Lake  | Raw and processed storage                  |
| Parquet          | Columnar data storage                      |
| Google BigQuery  | Cloud analytical warehouse                 |
| dbt              | SQL transformation and data quality        |
| Apache Airflow   | Workflow orchestration                     |
| Conda            | Python environment management              |
| Google Cloud SDK | GCP interaction                            |
| Terraform        | Infrastructure as Code                     |
| Git/GitHub       | Version control                            |
| GitHub Actions   | Continuous integration                     |

---

# 4. Data Flow

The complete data flow is:

```text
CoinGecko
    │
    │ REST API
    ▼
Python Extractor
    │
    │ JSON events
    ▼
Kafka Producer
    │
    ▼
Kafka Topic
crypto-market-data
    │
    ▼
Kafka Consumer
    │
    ├──────────────► Dead Letter Queue
    │
    ▼
Raw Data Lake
JSON
    │
    ▼
Pandas Transformation
    │
    ▼
Processed Data Lake
Parquet
    │
    ▼
BigQuery
crypto_prices
    │
    ▼
dbt
    │
    ├── staging
    ├── deduplication
    ├── freshness
    ├── tests
    └── marts
```

---

# 5. Kafka Architecture

Kafka introduces an event-streaming layer between data extraction and downstream processing.

## Kafka Topic

```text
crypto-market-data
```

The topic currently uses:

```text
Partitions: 3
Replication factor: 1
```

A separate topic is used for rejected events:

```text
crypto-market-data-dlq
```

The Dead Letter Queue allows invalid events to be isolated instead of stopping the entire processing flow.

The Kafka broker runs locally in Docker.

---

# 6. Event Envelope

Cryptocurrency records are wrapped in a standard event envelope before being published to Kafka.

Example:

```json
{
  "event_id": "bitcoin",
  "event_type": "crypto_market_update",
  "source": "coingecko",
  "received_at": "2026-09-27T10:00:00+00:00",
  "data": {
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "current_price": 84018.0
  }
}
```

The envelope separates event metadata from the cryptocurrency payload.

It provides fields such as:

* Event ID
* Event type
* Source
* Event reception timestamp
* Cryptocurrency payload

---

# 7. Data Lake

The project uses a local data lake to maintain a zero-cost development environment.

```text
data/
├── raw/
├── processed/
└── lake/
    ├── raw/
    │   └── crypto_market/
    └── processed/
        └── crypto_market/
            └── year=YYYY/
                └── month=MM/
                    └── day=DD/
                        └── crypto_market_TIMESTAMP.parquet
```

## Raw Layer

Raw Kafka events are stored as JSON:

```text
data/lake/raw/crypto_market/
```

The raw layer preserves the original event information received from Kafka.

## Processed Layer

Processed records are stored as Parquet:

```text
data/lake/processed/crypto_market/
```

The processed data uses date-based partitioning:

```text
year=2026/
month=09/
day=27/
```

This structure makes the local data lake suitable for later migration to cloud object storage such as Google Cloud Storage.

---

# 8. Parquet Timestamp Handling

The pipeline initially encountered a BigQuery compatibility problem because Pandas/PyArrow generated Parquet timestamps using nanosecond precision.

BigQuery reported:

```text
Invalid timestamp nanoseconds value
of logical type TIMESTAMP_NANOS
```

The transformation layer was updated to store the relevant timestamps using microsecond precision:

```python
df["received_at"] = (
    pd.to_datetime(df["received_at"], utc=True)
    .dt.tz_localize(None)
    .astype("datetime64[us]")
)

df["last_updated"] = (
    pd.to_datetime(df["last_updated"], utc=True)
    .dt.tz_localize(None)
    .astype("datetime64[us]")
)
```

This converts the timestamps to UTC, removes timezone metadata from the Parquet representation, and stores them using microsecond precision.

The resulting Parquet files load successfully into BigQuery.

---

# 9. BigQuery

Google BigQuery is used as the cloud analytical warehouse.

## GCP Project

```text
crypto-market-data-platform
```

## Dataset

```text
crypto_market
```

## Raw Table

```text
crypto_prices
```

The raw BigQuery table contains fields including:

```text
event_id
event_type
source
received_at
id
symbol
name
current_price
market_cap
market_cap_rank
total_volume
high_24h
low_24h
price_change_24h
price_change_percentage_24h
circulating_supply
total_supply
max_supply
ath
atl
last_updated
```

The BigQuery dataset and raw table are managed using Terraform.

The dataset uses a configured 60-day default expiration policy for tables and partitions where applicable, supporting the zero-cost development approach.

---

# 10. dbt

dbt provides the SQL transformation and data-quality layer.

Project:

```text
crypto_dbt/
```

The dbt transformation flow is:

```text
BigQuery raw table
        │
        ▼
stg_crypto_prices
        │
        ├── validation
        ├── deduplication
        └── freshness
        │
        ▼
mart_crypto_latest
```

## Staging Model

```text
stg_crypto_prices
```

The staging model:

* Normalizes cryptocurrency fields.
* Renames `id` to `crypto_id`.
* Lowercases cryptocurrency symbols.
* Removes duplicate records.
* Keeps the latest received record for a cryptocurrency/timestamp combination.

## Mart Model

```text
mart_crypto_latest
```

The mart contains the latest analytical record for the cryptocurrencies being processed.

## dbt Tests

The project contains automated dbt data-quality checks covering:

* Uniqueness
* Valid values
* Staging data
* Mart data
* Freshness

The completed dbt test suite achieved:

```text
17/17 tests passing
```

dbt manages the staging and mart models, while Terraform manages the underlying BigQuery dataset and raw table. This avoids infrastructure ownership conflicts between Terraform and dbt.

---

# 11. Airflow

Apache Airflow orchestrates the complete pipeline.

DAG:

```text
crypto_market_pipeline
```

The DAG contains six tasks:

```text
extract_from_coingecko
        │
        ▼
produce_to_kafka
        │
        ▼
consume_from_kafka
        │
        ▼
transform_to_parquet
        │
        ▼
load_to_bigquery
        │
        ▼
run_dbt
```

The DAG is currently configured for manual execution:

```python
schedule=None
```

This allows the complete pipeline to be triggered manually during development.

The project uses:

```text
Apache Airflow 3.3.2
```

The CryptoData project uses its own Airflow home:

```bash
export AIRFLOW_HOME="$HOME/Desktop/CryptoData/airflow"
```

This keeps the project's Airflow metadata separate from other Airflow projects on the machine.

---

# 12. Airflow Execution

The pipeline has been successfully executed end-to-end.

Successful run:

```text
manual__2026-09-27T11:15:28.629946+00:00
```

Execution result:

```text
extract_from_coingecko    SUCCESS
produce_to_kafka          SUCCESS
consume_from_kafka        SUCCESS
transform_to_parquet      SUCCESS
load_to_bigquery          SUCCESS
run_dbt                   SUCCESS
```

The complete workflow was:

```text
CoinGecko API
     │
     ▼
extract_from_coingecko       ✅
     │
     ▼
produce_to_kafka              ✅
     │
     ▼
consume_from_kafka            ✅
     │
     ▼
transform_to_parquet          ✅
     │
     ▼
load_to_bigquery              ✅
     │
     ▼
run_dbt                       ✅
```

Result:

```text
6/6 Airflow tasks successful
```

This confirms that the complete pipeline works from external API ingestion through Kafka, local storage, BigQuery, and dbt transformation.

---

# 13. Infrastructure as Code

Terraform is used to manage the BigQuery infrastructure.

Terraform version used during development:

```text
Terraform v1.16.4
```

Terraform manages:

```text
GCP Project
     │
     ▼
BigQuery Dataset
crypto_market
     │
     ▼
Raw Table
crypto_prices
```

Terraform configuration is stored in:

```text
terraform/
```

Main Terraform files:

```text
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── versions.tf
└── .terraform.lock.hcl
```

The Terraform configuration defines:

* Google Cloud provider
* BigQuery dataset
* BigQuery raw table
* Dataset expiration settings
* Table schema
* Terraform outputs

Terraform validation is also performed automatically by GitHub Actions.

The Terraform infrastructure was successfully imported and reconciled with the existing BigQuery resources.

Current Terraform state:

```text
Terraform infrastructure       ✅
Terraform initialization       ✅
Terraform validation           ✅
Terraform plan                 ✅
Terraform apply                ✅
```

Terraform and dbt have deliberately separated responsibilities:

```text
Terraform
    │
    ├── BigQuery dataset
    └── Raw table
            │
            ▼
           dbt
            │
            ├── staging view
            └── analytical mart
```

---

# 14. CI/CD

GitHub Actions provides automated continuous integration.

Workflow:

```text
.github/workflows/ci.yml
```

The CI pipeline runs three independent jobs:

```text
                    Git push / Pull Request
                              │
                              ▼
                     GitHub Actions
                              │
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
       Python checks    Terraform checks    dbt checks
             │                │                │
             ▼                ▼                ▼
          Compile          Format          Parse
          Tests            Validate
             │                │                │
             └────────────────┼────────────────┘
                              ▼
                         CI SUCCESS
```

## Python Checks

The Python CI job:

* Sets up Python 3.12.
* Installs development dependencies.
* Compiles the source code.
* Runs the Python test suite.

The project contains:

```text
9 Python unit tests
```

Current result:

```text
9/9 tests passing
```

## Terraform Checks

The Terraform CI job:

```text
terraform fmt -check
terraform init -backend=false
terraform validate
```

This validates the Terraform configuration without requiring a remote Terraform backend.

## dbt Checks

The dbt CI job:

* Installs dbt 2.0.6.
* Creates an isolated CI dbt profile.
* Runs `dbt parse`.

The CI environment does not contain production GCP credentials, so the CI job validates the dbt project structure without attempting to modify BigQuery.

## Dependency Separation

The project separates dependencies by responsibility.

### Runtime Dependencies

```text
requirements.txt
```

Contains application dependencies such as:

```text
requests
pydantic-settings
pandas
pyarrow
kafka-python
google-cloud-bigquery
```

### Development Dependencies

```text
requirements-dev.txt
```

Includes runtime dependencies plus:

```text
pytest
```

### dbt Dependencies

```text
requirements-dbt.txt
```

Contains:

```text
dbt==2.0.6
```

### Airflow Dependencies

```text
requirements-airflow.txt
```

Contains:

```text
apache-airflow==3.3.2
```

The Google Cloud CLI is installed as a system/tool dependency and is not listed as a Python package because `google-cloud-cli` is not a pip package.

## CI Result

The latest CI implementation successfully passed all three jobs:

```text
Python checks       ✅
Terraform checks    ✅
dbt checks          ✅
────────────────────────
CI pipeline         ✅ SUCCESS
```

---

# 15. Project Structure

```text
CryptoData/
│
├── .env
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── requirements-dbt.txt
├── requirements-airflow.txt
├── docker-compose.yml
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── .vscode/
│   └── settings.json
│
├── src/
│   ├── __init__.py
│   │
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   │
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── coingecko_client.py
│   │   ├── main.py
│   │   ├── transform.py
│   │   ├── inspect.py
│   │   └── validate.py
│   │
│   ├── kafka/
│   │   ├── __init__.py
│   │   ├── producer.py
│   │   ├── consumer.py
│   │   ├── processor.py
│   │   ├── dlq.py
│   │   └── envelope.py
│   │
│   ├── lake/
│   │   ├── __init__.py
│   │   ├── storage.py
│   │   └── transform.py
│   │
│   ├── bigquery/
│   │   ├── __init__.py
│   │   └── loader.py
│   │
│   └── utils/
│       ├── __init__.py
│       └── logger.py
│
├── tests/
│   ├── __init__.py
│   └── test_validate.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── lake/
│
├── logs/
│
├── docs/
│
├── scripts/
│
├── airflow/
│   ├── dags/
│   │   └── crypto_market_pipeline.py
│   ├── logs/
│   └── plugins/
│
├── crypto_dbt/
│   ├── dbt_project.yml
│   ├── models/
│   ├── tests/
│   └── ...
│
└── terraform/
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── versions.tf
    └── .terraform.lock.hcl
```

### Generated and local-only files

The following are generated during development and are not tracked as source code:

```text
.env
airflow/airflow.db
airflow/airflow.db-shm
airflow/airflow.db-wal
airflow/logs/
data/lake/raw/
data/lake/processed/
Python cache files
dbt target artifacts
Terraform state
.terraform/
```

The repository tracks reproducible source and configuration files rather than runtime artifacts.

---

# 16. Environment Setup

Create and activate the Conda environment:

```bash
conda create -n crypto_market python=3.12
conda activate crypto_market
```

Install runtime dependencies:

```bash
pip install -r requirements.txt
```

For development and testing:

```bash
pip install -r requirements-dev.txt
```

Install dbt separately:

```bash
pip install -r requirements-dbt.txt
```

Install Airflow separately:

```bash
pip install -r requirements-airflow.txt
```

The project uses Conda where practical to reduce binary compatibility issues between:

* NumPy
* Pandas
* PyArrow

Verify the Python environment:

```bash
python --version
```

Expected:

```text
Python 3.12.x
```

---

# 17. Start Kafka

Kafka runs locally using Docker.

Check Docker:

```bash
sudo systemctl status docker
```

Start the existing Kafka container:

```bash
docker start crypto-kafka
```

Verify:

```bash
docker ps --filter "name=crypto-kafka"
```

Expected container:

```text
crypto-kafka
```

---

# 18. Test CoinGecko Extraction

Run:

```bash
python -c "from src.ingestion.coingecko_client import CoinGeckoClient; data=CoinGeckoClient().get_market_data(coin_ids=['bitcoin','ethereum','solana']); print(f'Retrieved {len(data)} coins'); print([coin['id'] for coin in data])"
```

Expected result:

```text
Retrieved 3 coins
['bitcoin', 'ethereum', 'solana']
```

---

# 19. Start Airflow

Set the project-specific Airflow home:

```bash
export AIRFLOW_HOME="$HOME/Desktop/CryptoData/airflow"
```

Start Airflow:

```bash
airflow standalone
```

Verify the scheduler:

```bash
airflow jobs check --job-type SchedulerJob
```

Expected:

```text
Found one alive job.
```

---

# 20. Trigger the Pipeline

Trigger the DAG:

```bash
airflow dags trigger crypto_market_pipeline
```

Check task states:

```bash
airflow tasks states-for-dag-run \
crypto_market_pipeline \
<RUN_ID>
```

A successful run should show:

```text
extract_from_coingecko    success
produce_to_kafka          success
consume_from_kafka        success
transform_to_parquet      success
load_to_bigquery          success
run_dbt                   success
```

---

# 21. Run Tests

Run the Python test suite:

```bash
pytest -v
```

Expected result:

```text
9 passed
```

Compile the Python source:

```bash
python -m compileall src
```

---

# 22. Useful Validation Commands

## Check Kafka

```bash
docker ps --filter "name=crypto-kafka"
```

## Check Parquet Files

```bash
find data/lake/processed/crypto_market -name "*.parquet"
```

## Check BigQuery Rows

```bash
bq query --use_legacy_sql=false \
"SELECT COUNT(*) AS row_count
 FROM \`crypto-market-data-platform.crypto_market.crypto_prices\`"
```

## Check dbt

```bash
cd crypto_dbt

dbt debug
dbt build
```

## Check Terraform

```bash
cd terraform

terraform fmt -check
terraform validate
terraform plan
```

---

# 23. Data Quality

The pipeline implements multiple layers of data-quality control.

```text
CoinGecko API
     │
     ▼
Extraction
validation
     │
     ▼
Kafka event
validation
     │
     ├── Valid ─────────────► Data Lake
     │
     └── Invalid ───────────► DLQ
                              │
                              ▼
                    crypto-market-data-dlq
     │
     ▼
dbt validation
     │
     ├── uniqueness
     ├── valid values
     ├── freshness
     └── deduplication
```

The Python validation layer checks conditions such as:

* Required columns
* Required values
* Numeric ranges
* Duplicate cryptocurrency IDs
* Timestamp validity
* Timestamp freshness

The project contains:

```text
9 Python unit tests
17 dbt tests
```

This creates multiple quality gates before data reaches analytical models.

---

# 24. Engineering Concepts Demonstrated

This project demonstrates practical knowledge of modern data engineering.

## Data Ingestion

* REST APIs
* HTTP requests
* API parameters
* JSON
* Optional API authentication
* Error handling
* Configuration management

## Streaming

* Kafka producers
* Kafka consumers
* Topics
* Partitions
* Offsets
* Consumer groups
* Event envelopes
* Dead Letter Queues

## Data Lakes

* Raw data
* Processed data
* JSON
* Parquet
* Partitioned storage
* Layered data storage

## Data Transformation

* Pandas
* PyArrow
* Timestamp handling
* Schema normalization
* Data validation

## Data Warehousing

* Google BigQuery
* Datasets
* Tables
* Schema management
* Parquet ingestion
* Warehouse data modeling

## Analytics Engineering

* dbt models
* Staging models
* Mart models
* SQL transformations
* Deduplication
* Data-quality tests
* Freshness checks

## Orchestration

* Apache Airflow
* DAGs
* PythonOperator
* Task dependencies
* XCom
* End-to-end workflow execution

## Infrastructure

* Docker
* Kafka containers
* Conda environments
* Google Cloud SDK
* Terraform
* Infrastructure as Code

## DevOps / CI

* Git
* GitHub
* GitHub Actions
* Automated Python testing
* Terraform validation
* dbt project validation
* Dependency separation

---

# 25. Key Design Decisions

## Why Kafka?

Kafka provides a streaming layer between ingestion and downstream processing.

Instead of:

```text
API → Database
```

the architecture becomes:

```text
API → Kafka → Consumers → Data Lake/Warehouse
```

This creates a foundation for future continuous and real-time workloads.

The current project manually triggers the overall Airflow workflow, so Kafka provides the streaming architecture without claiming that the entire platform currently runs continuously.

## Why a Local Data Lake?

A local data lake keeps the project zero-cost while demonstrating important data-lake concepts.

The local implementation can later be migrated to cloud object storage such as Google Cloud Storage.

## Why Parquet?

Parquet is a columnar storage format that is efficient for analytical workloads and generally more compact and query-friendly than raw JSON.

## Why BigQuery?

BigQuery provides a cloud-native analytical warehouse without requiring a locally managed database server.

The project uses the BigQuery Sandbox/zero-cost development approach rather than depending on paid infrastructure.

## Why dbt?

dbt separates warehouse transformation logic from ingestion code.

It provides:

* SQL models
* Testing
* Freshness checks
* Deduplication
* Reusable transformations
* Analytical marts

## Why Airflow?

Airflow coordinates the complete workflow and makes task dependencies explicit:

```text
Extract
  ↓
Stream
  ↓
Consume
  ↓
Transform
  ↓
Load
  ↓
Transform/Test
```

## Why Terraform?

Terraform makes the cloud infrastructure reproducible and version-controlled.

Instead of manually creating BigQuery resources, infrastructure is represented as code:

```text
Terraform configuration
        ↓
Terraform plan
        ↓
Terraform apply
        ↓
GCP resources
```

## Why GitHub Actions?

GitHub Actions automatically checks changes before they are accepted as healthy project changes.

The current CI pipeline validates:

```text
Python
  ↓
Tests

Terraform
  ↓
Format + Validate

dbt
  ↓
Parse
```

This reduces the risk of introducing broken code or invalid infrastructure configuration.

---

# 26. Current Project Status

```text
CoinGecko API              ✅
Python ingestion           ✅
Raw JSON                   ✅
Kafka                      ✅
Kafka Producer             ✅
Kafka Consumer             ✅
Dead Letter Queue          ✅
Local Data Lake            ✅
Pandas transformation      ✅
Parquet                    ✅
BigQuery                   ✅
dbt staging                ✅
dbt deduplication          ✅
dbt freshness              ✅
dbt tests                  ✅
dbt marts                  ✅
Airflow DAG                ✅
Airflow orchestration      ✅
Docker                     ✅
End-to-end execution       ✅
Terraform                  ✅
CI/CD                      ✅
Python unit tests          ✅
Dashboard                  ⬜
Additional monitoring      ⬜
```

Current validation results:

```text
Airflow tasks:       6/6 successful
Python tests:        9/9 successful
dbt tests:          17/17 successful
Terraform checks:    PASS
GitHub Actions CI:   SUCCESS
```

---

# 27. Future Improvements

The core end-to-end platform is now implemented. Future work will focus on turning the engineering pipeline into a more complete production-style analytics platform.

## Dashboard

Build an analytical dashboard displaying:

* Cryptocurrency prices
* Market capitalization
* Trading volume
* 24-hour price changes
* Market rankings
* Historical trends

The dashboard is the next major project component.

## Scheduling

Change the Airflow DAG from:

```python
schedule=None
```

to a periodic schedule such as:

```text
Every 15 minutes
```

This would move the project closer to continuous market-data ingestion.

## Monitoring

Add:

* Airflow alerts
* Pipeline metrics
* Kafka monitoring
* Data-quality alerts
* Failure notifications
* Logging dashboards

## Cloud Data Lake

Replace or complement the local data lake with:

```text
Google Cloud Storage
```

while retaining the same raw/processed data-layer concepts.

## Security

Add:

* Dedicated service accounts
* IAM roles
* Secret Manager
* More restrictive permissions
* Secure API-key management

## Advanced Streaming

Extend the Kafka implementation with:

* More cryptocurrency assets
* Continuous producers
* Multiple consumer groups
* Stream processing
* Event-time processing
* Higher-volume workloads

## Deployment

Extend CI/CD beyond validation toward deployment of:

* Airflow components
* Data infrastructure
* dbt models
* Cloud resources

---

# 28. Project Outcome

The Cryptocurrency Market Data Platform demonstrates a complete modern data engineering workflow:

```text
External API
     ↓
Python
     ↓
Kafka
     ↓
Data Lake
     ↓
Parquet
     ↓
BigQuery
     ↓
dbt
     ↓
Airflow
     ↓
CI/CD
```

The complete Airflow pipeline has been successfully executed with all six tasks passing:

```text
6/6 tasks successful
```

The project also has automated validation:

```text
9/9 Python tests passing
17/17 dbt tests passing
Terraform validation passing
GitHub Actions CI passing
```

The completed platform demonstrates practical experience with:

* API ingestion
* Event streaming
* Kafka
* Dead Letter Queues
* Data lakes
* Parquet
* Pandas
* BigQuery
* SQL transformation
* dbt
* Data quality
* Airflow orchestration
* Docker
* Terraform
* Git/GitHub
* GitHub Actions CI/CD

The next major component is the **analytics dashboard**, which will expose the processed cryptocurrency data through a user-facing analytical interface.
