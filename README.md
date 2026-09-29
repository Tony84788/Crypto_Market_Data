# Cryptocurrency Market Data Platform

An end-to-end cryptocurrency market data engineering platform that extracts market data from the CoinGecko API, streams events through Apache Kafka, stores raw and processed data in a local data lake, loads analytical data into Google BigQuery, transforms and tests warehouse data with dbt, orchestrates the complete workflow with Apache Airflow, manages cloud infrastructure with Terraform, validates changes through GitHub Actions CI, and exposes analytical data through a Streamlit dashboard.

The project is designed as a **zero-cost learning and portfolio platform**, combining local infrastructure, Docker, Kafka, a local data lake, and the Google BigQuery Sandbox.

The pipeline is configured for **hourly execution through Airflow** and has been successfully validated end-to-end.

---

## Architecture

```text
                              ┌──────────────────────┐
                              │     CoinGecko API    │
                              │                      │
                              │ Cryptocurrency       │
                              │ Market Data          │
                              └──────────┬───────────┘
                                         │
                                         │ REST API
                                         ▼
                              ┌──────────────────────┐
                              │   Python Extractor   │
                              │                      │
                              │ requests             │
                              │ configuration        │
                              │ validation           │
                              └──────────┬───────────┘
                                         │
                                         │ JSON events
                                         ▼
                              ┌──────────────────────┐
                              │    Apache Kafka      │
                              │                      │
                              │ crypto-market-data   │
                              └──────────┬───────────┘
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │   Kafka Consumer     │
                              │                      │
                              │ validation           │
                              │ processing            │
                              │ DLQ handling          │
                              └──────────┬───────────┘
                                         │
                         ┌───────────────┴───────────────┐
                         │                               │
                       Valid                         Invalid
                         │                               │
                         ▼                               ▼
              ┌──────────────────────┐       ┌──────────────────────┐
              │    Local Data Lake   │       │ Dead Letter Queue    │
              │                      │       │                      │
              │ Raw JSON             │       │ crypto-market-data-  │
              │        ↓             │       │ dlq                  │
              │ Processed Parquet    │       └──────────────────────┘
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │     Google BigQuery  │
              │                      │
              │ crypto_market        │
              │        │             │
              │ crypto_prices        │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │         dbt          │
              │                      │
              │ staging              │
              │ deduplication        │
              │ freshness            │
              │ tests                │
              │ analytical marts     │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │ Streamlit Dashboard  │
              │                      │
              │ Market overview      │
              │ Price performance    │
              │ Historical prices    │
              │ Data freshness       │
              └──────────────────────┘


              ┌─────────────────────────────────────────┐
              │              Apache Airflow              │
              │                                         │
              │     Orchestrates the complete pipeline  │
              │     on an hourly schedule               │
              └─────────────────────────────────────────┘

              ┌─────────────────────────────────────────┐
              │ Terraform + GitHub Actions              │
              │                                         │
              │ Infrastructure as Code + CI/CD          │
              └─────────────────────────────────────────┘
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
9. Transform warehouse data using dbt.
10. Apply data-quality tests, freshness checks, and deduplication.
11. Create analytical marts.
12. Orchestrate the complete workflow using Apache Airflow.
13. Manage BigQuery infrastructure using Terraform.
14. Validate Python, Terraform, and dbt components through GitHub Actions.
15. Present analytical results through a Streamlit dashboard.

The current pipeline processes:

```text
Bitcoin
Ethereum
Solana
```

---

# 2. Data Source

## CoinGecko

CoinGecko is the external cryptocurrency market data provider used by the platform.

API documentation:

https://docs.coingecko.com/

Primary endpoint:

```text
https://api.coingecko.com/api/v3/coins/markets
```

The API provides market information including:

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

Secrets are stored locally in `.env` and are excluded from version control.

---

# 3. Technology Stack

| Technology       | Purpose                                                    |
| ---------------- | ---------------------------------------------------------- |
| Python           | Data ingestion, validation, transformation, and processing |
| Requests         | CoinGecko API communication                                |
| Pandas           | Data transformation                                        |
| PyArrow          | Parquet processing                                         |
| Apache Kafka     | Event streaming                                            |
| Docker           | Local Kafka infrastructure                                 |
| Local Data Lake  | Raw and processed data storage                             |
| JSON             | Raw event storage                                          |
| Parquet          | Processed columnar storage                                 |
| Google BigQuery  | Analytical data warehouse                                  |
| dbt              | SQL transformation and data quality                        |
| Apache Airflow   | Workflow orchestration and scheduling                      |
| Streamlit        | Analytical dashboard                                       |
| Plotly           | Dashboard visualizations                                   |
| Conda            | Python environment management                              |
| Google Cloud SDK | GCP interaction                                            |
| Terraform        | Infrastructure as Code                                     |
| Git/GitHub       | Version control                                            |
| GitHub Actions   | Continuous integration                                     |

---

# 4. Complete Data Flow

```text
CoinGecko API
      │
      │ REST API
      ▼
Python Extractor
      │
      │ JSON
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
      ├──────────────► Invalid Events
      │                       │
      │                       ▼
      │                  Dead Letter Queue
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
      │
      ▼
Streamlit Dashboard
```

Airflow orchestrates the complete sequence.

---

# 5. Kafka Architecture

Kafka provides the event-streaming layer between data extraction and downstream processing.

## Main Topic

```text
crypto-market-data
```

Configuration:

```text
Partitions: 3
Replication factor: 1
```

## Dead Letter Queue

Invalid or rejected events are routed to:

```text
crypto-market-data-dlq
```

The DLQ prevents invalid events from unnecessarily stopping the processing pipeline and provides a location for later inspection or reprocessing.

Kafka runs locally inside Docker.

---

# 6. Kafka Event Envelope

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

It contains information such as:

* Event ID
* Event type
* Source
* Event reception timestamp
* Cryptocurrency payload

This creates a consistent structure for downstream Kafka consumers.

---

# 7. Local Data Lake

The project uses a local data lake to maintain a zero-cost development environment while demonstrating data-lake architecture.

```text
data/
└── lake/
    ├── raw/
    │   └── crypto_market/
    │
    └── processed/
        └── crypto_market/
            └── year=YYYY/
                └── month=MM/
                    └── day=DD/
                        └── crypto_market_TIMESTAMP.parquet
```

## Raw Layer

Raw Kafka events are stored under:

```text
data/lake/raw/crypto_market/
```

The raw layer preserves the original event information received from Kafka.

## Processed Layer

Processed records are stored under:

```text
data/lake/processed/crypto_market/
```

Processed data is stored in Parquet format and partitioned by date:

```text
year=2026/
month=09/
day=27/
```

The architecture can later be migrated from local storage to cloud object storage such as Google Cloud Storage without changing the conceptual raw/processed data-layer design.

---

# 8. Parquet Timestamp Handling

During development, the pipeline encountered a BigQuery compatibility issue caused by Parquet timestamps using nanosecond precision.

BigQuery reported:

```text
Invalid timestamp nanoseconds value
of logical type TIMESTAMP_NANOS
```

The transformation layer was updated to use microsecond timestamp precision:

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

This demonstrates practical handling of **cross-system data-type compatibility** between Pandas, PyArrow, Parquet, and BigQuery.

---

# 9. BigQuery

Google BigQuery is used as the cloud analytical warehouse.

## Existing GCP Project

```text
crypto-market-data-platform
```

Terraform manages BigQuery resources **inside this existing GCP project**.

## Dataset

```text
crypto_market
```

## Raw Table

```text
crypto_prices
```

The raw table contains fields including:

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

The BigQuery dataset uses a configured 60-day expiration policy for the zero-cost development environment.

---

# 10. dbt

dbt provides the SQL transformation and data-quality layer.

Project:

```text
crypto_dbt/
```

The transformation flow is:

```text
BigQuery
crypto_prices
      │
      ▼
stg_crypto_prices
      │
      ├── normalization
      ├── deduplication
      └── freshness validation
      │
      ▼
Analytical Models
      │
      ├── mart_crypto_latest
      └── fct_crypto_price_history
```

## Staging Model

```text
stg_crypto_prices
```

The staging model:

* Normalizes cryptocurrency fields.
* Renames `id` to `crypto_id`.
* Normalizes cryptocurrency symbols.
* Removes duplicate records.
* Retains the appropriate latest records.

## Latest Mart

```text
mart_crypto_latest
```

This model provides the latest analytical record for the cryptocurrencies being processed.

## Historical Mart

```text
fct_crypto_price_history
```

This model provides historical cryptocurrency price records for analytical workloads.

## dbt Tests

The project contains automated data-quality checks covering:

* Uniqueness
* Valid values
* Staging data
* Mart data
* Freshness
* Data integrity

Validation result:

```text
17/17 dbt tests passing
```

Terraform and dbt deliberately have separate responsibilities:

```text
Terraform
    │
    ├── BigQuery dataset
    └── Raw BigQuery table
             │
             ▼
            dbt
             │
             ├── staging
             └── analytical marts
```

This prevents infrastructure ownership conflicts.

---

# 11. Apache Airflow

Apache Airflow orchestrates the complete data pipeline.

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

## Airflow Schedule

The pipeline is configured to run hourly:

```text
0 * * * *
```

Meaning:

```text
Run at minute 0 of every hour.
```

Airflow version used during development:

```text
Apache Airflow 3.3.2
```

The CryptoData project uses its own Airflow home:

```bash
export AIRFLOW_HOME="$HOME/Desktop/CryptoData/airflow"
```

This keeps the project's Airflow metadata and logs separate from other Airflow projects on the machine.

---

# 12. Airflow Execution

The pipeline has been successfully executed end-to-end.

A successful manual run:

```text
manual__2026-09-27T11:15:28.629946+00:00
```

All six tasks succeeded:

```text
extract_from_coingecko    SUCCESS
produce_to_kafka          SUCCESS
consume_from_kafka        SUCCESS
transform_to_parquet      SUCCESS
load_to_bigquery          SUCCESS
run_dbt                   SUCCESS
```

Result:

```text
6/6 Airflow tasks successful
```

The project subsequently verified the scheduled workflow with a successful scheduled run:

```text
scheduled__2026-09-29T09:00:00+00:00
```

Scheduled task results:

```text
extract_from_coingecko    SUCCESS
produce_to_kafka          SUCCESS
consume_from_kafka        SUCCESS
transform_to_parquet      SUCCESS
load_to_bigquery          SUCCESS
run_dbt                   SUCCESS
```

This confirms that the Airflow schedule successfully executes the complete pipeline.

---

# 13. Pipeline Monitoring and Data Quality

The pipeline contains multiple validation layers.

## Extraction Monitoring

The Airflow extraction task checks that CoinGecko returns records.

```python
record_count = len(crypto_data)

if record_count == 0:
    raise ValueError(
        "Monitoring check failed: CoinGecko returned 0 records."
    )

print(
    f"Monitoring check passed: "
    f"{record_count} records retrieved."
)
```

This prevents an empty API response from silently propagating through the pipeline.

## Python Validation

The Python validation layer checks:

* Required columns
* Required values
* Numeric ranges
* Duplicate cryptocurrency IDs
* Timestamp validity
* Timestamp freshness

## Kafka Validation

Kafka processing validates events before they enter the downstream data lake.

Invalid events can be routed to:

```text
crypto-market-data-dlq
```

## dbt Validation

dbt provides warehouse-level:

* Uniqueness checks
* Valid-value checks
* Freshness checks
* Model tests
* Deduplication

The project contains:

```text
9 Python unit tests
17 dbt tests
```

Validation results:

```text
9/9 Python tests passing
17/17 dbt tests passing
```

---

# 14. Infrastructure as Code

Terraform is used to manage BigQuery infrastructure.

Terraform version:

```text
Terraform v1.16.4
```

Google provider:

```text
hashicorp/google
```

Terraform configuration:

```text
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── versions.tf
└── .terraform.lock.hcl
```

Terraform manages:

```text
Existing GCP Project
        │
        ▼
BigQuery Dataset
crypto_market
        │
        ▼
Raw Table
crypto_prices
```

Terraform configuration defines:

* Google Cloud provider
* BigQuery dataset
* BigQuery raw table
* Dataset/table expiration configuration
* Raw table schema
* Terraform outputs

The infrastructure was imported and reconciled with the existing BigQuery resources.

Terraform validation results:

```text
Terraform initialization       PASS
Terraform formatting          PASS
Terraform validation          PASS
Terraform plan                NO CHANGES
Terraform apply               0 added
                               0 changed
                               0 destroyed
```

---

# 15. CI/CD

GitHub Actions provides automated continuous integration.

Workflow:

```text
.github/workflows/ci.yml
```

The CI pipeline contains three independent jobs:

```text
                 Git Push / Pull Request
                          │
                          ▼
                   GitHub Actions
                          │
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
    Python Checks   Terraform Checks    dbt Checks
          │               │                │
          ▼               ▼                ▼
       Compile          Format           Parse
       Tests            Validate
          │               │                │
          └───────────────┼────────────────┘
                          ▼
                     CI SUCCESS
```

## Python Checks

The Python CI job:

* Sets up Python 3.12.
* Installs development dependencies.
* Compiles the Python source.
* Runs the test suite.

Result:

```text
9/9 Python tests passing
```

## Terraform Checks

The Terraform job runs:

```bash
terraform fmt -check
terraform init -backend=false
terraform validate
```

## dbt Checks

The dbt CI job:

* Installs dbt 2.0.6.
* Creates an isolated CI profile.
* Runs `dbt parse`.

The CI environment does not contain production GCP credentials and therefore validates the dbt project structure without modifying BigQuery.

## Dependency Separation

Runtime dependencies:

```text
requirements.txt
```

Development dependencies:

```text
requirements-dev.txt
```

dbt dependencies:

```text
requirements-dbt.txt
```

Airflow dependencies:

```text
requirements-airflow.txt
```

Dashboard dependencies:

```text
requirements-dashboard.txt
```

The project separates dependencies by responsibility rather than placing every tool into one environment.

## CI Result

The latest CI pipeline passed:

```text
Python checks       ✅
Terraform checks    ✅
dbt checks          ✅
────────────────────────
CI pipeline         ✅ SUCCESS
```

---

# 16. Streamlit Dashboard

The project includes a Streamlit analytical dashboard.

Application:

```text
dashboard/app.py
```

Dashboard dependencies:

```text
streamlit==1.64.0
plotly==7.1.0
```

The dashboard provides:

* Refresh Data
* Data freshness monitoring
* Market overview KPIs
* 24-hour price performance
* Market capitalization
* 24-hour trading volume
* Historical price selection
* Latest cryptocurrency data
* Missing-data handling
* BigQuery error handling
* Partial-data handling
* Responsive wide-layout presentation

## Data Freshness Categories

The dashboard categorizes data as:

```text
Fresh       ≤ 10 minutes
Aging       10–30 minutes
Stale       > 30 minutes
```

The dashboard consumes analytical data from BigQuery rather than directly querying the external CoinGecko API for every visualization.

---

# 17. Project Structure

```text
CryptoData/
│
├── .env
├── .env.example
├── .gitignore
├── README.md
│
├── requirements.txt
├── requirements-dev.txt
├── requirements-dbt.txt
├── requirements-airflow.txt
├── requirements-dashboard.txt
│
├── docker-compose.yml
│
├── .github/
│   └── workflows/
│       └── ci.yml
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
├── dashboard/
│   └── app.py
│
├── data/
│   └── lake/
│       ├── raw/
│       └── processed/
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

## Generated and Local-Only Files

The following files/directories are generated locally and are not tracked as project source:

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

The repository tracks reproducible source code and configuration rather than runtime artifacts or secrets.

---

# 18. Environment Setup

Create the Conda environment:

```bash
conda create -n crypto_market python=3.12
```

Activate it:

```bash
conda activate crypto_market
```

Install runtime dependencies:

```bash
pip install -r requirements.txt
```

Install development dependencies:

```bash
pip install -r requirements-dev.txt
```

Install dbt:

```bash
pip install -r requirements-dbt.txt
```

Install Airflow:

```bash
pip install -r requirements-airflow.txt
```

Install dashboard dependencies:

```bash
pip install -r requirements-dashboard.txt
```

The project uses Conda where practical to reduce binary compatibility issues between packages such as:

* NumPy
* Pandas
* PyArrow

Verify Python:

```bash
python --version
```

Expected:

```text
Python 3.12.x
```

---

# 19. Environment Configuration

Create the local environment file:

```text
.env
```

Example:

```text
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
COINGECKO_API_KEY=your_api_key_here
```

Do **not** commit `.env` to Git.

The project includes:

```text
.env.example
```

for documenting required environment variables without exposing secrets.

---

# 20. Start Kafka

Kafka runs locally using Docker.

Check Docker:

```bash
sudo systemctl status docker
```

Start the Kafka container:

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

# 21. Test CoinGecko Extraction

Run:

```bash
python -c "from src.ingestion.coingecko_client import CoinGeckoClient; data=CoinGeckoClient().get_market_data(coin_ids=['bitcoin','ethereum','solana']); print(f'Retrieved {len(data)} coins'); print([coin['id'] for coin in data])"
```

Expected:

```text
Retrieved 3 coins

['bitcoin', 'ethereum', 'solana']
```

---

# 22. Start Airflow

Set the project-specific Airflow home:

```bash
export AIRFLOW_HOME="$HOME/Desktop/CryptoData/airflow"
```

Start Airflow:

```bash
airflow standalone
```

Airflow standalone starts the components required for the local development environment.

The project uses this dedicated Airflow environment so it does not interfere with other Airflow projects on the machine.

---

# 23. Trigger the Pipeline Manually

Although the pipeline is configured for hourly scheduling, it can also be triggered manually.

Run:

```bash
airflow dags trigger crypto_market_pipeline
```

Check DAG runs:

```bash
airflow dags list-runs -d crypto_market_pipeline
```

Check task states:

```bash
airflow tasks states-for-dag-run \
crypto_market_pipeline \
<RUN_ID>
```

A successful run should contain:

```text
extract_from_coingecko    success
produce_to_kafka          success
consume_from_kafka        success
transform_to_parquet      success
load_to_bigquery          success
run_dbt                   success
```

---

# 24. Run Tests

Run the Python test suite:

```bash
pytest -v
```

Expected:

```text
9 passed
```

Compile the Python source:

```bash
python -m compileall src
```

---

# 25. Useful Validation Commands

## Check Kafka

```bash
docker ps --filter "name=crypto-kafka"
```

## Check Processed Parquet Files

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

# 26. Data Quality Architecture

The platform implements data quality at multiple stages.

```text
CoinGecko API
      │
      ▼
Python Extraction
      │
      ├── required values
      ├── numeric validation
      └── timestamp validation
      │
      ▼
Kafka Event
      │
      ├── Valid
      │     │
      │     ▼
      │   Data Lake
      │
      └── Invalid
            │
            ▼
      Dead Letter Queue
      crypto-market-data-dlq
           

Data Lake
    │
    ▼
Parquet
    │
    ▼
BigQuery
    │
    ▼
dbt
    │
    ├── uniqueness
    ├── valid values
    ├── freshness
    ├── deduplication
    └── analytical model tests
```

Validation results:

```text
Python unit tests       9/9   PASS
dbt tests              17/17  PASS
Terraform validation           PASS
GitHub Actions CI              PASS
Airflow tasks             6/6  PASS
```

---

# 27. Engineering Concepts Demonstrated

## Data Ingestion

* REST APIs
* HTTP requests
* API parameters
* JSON
* API authentication
* Error handling
* Configuration management
* Environment variables

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
* Date partitioning
* Layered storage

## Data Transformation

* Pandas
* PyArrow
* Schema normalization
* Timestamp conversion
* Data validation
* Parquet generation

## Data Warehousing

* Google BigQuery
* Datasets
* Tables
* Schema management
* Parquet ingestion
* Analytical data modeling

## Analytics Engineering

* dbt
* SQL models
* Staging models
* Analytical marts
* Deduplication
* Freshness checks
* Data-quality tests

## Orchestration

* Apache Airflow
* DAGs
* PythonOperator
* Task dependencies
* XCom
* Scheduling
* End-to-end workflow execution

## Infrastructure

* Docker
* Kafka containers
* Conda
* Google Cloud SDK
* Terraform
* Infrastructure as Code

## DevOps / CI/CD

* Git
* GitHub
* GitHub Actions
* Automated Python testing
* Terraform validation
* dbt validation
* Dependency separation
* Secret protection

## Analytics

* Streamlit
* Plotly
* KPI dashboards
* Historical trends
* Data freshness visualization
* BigQuery-backed analytics

---

# 28. Key Design Decisions

## Why CoinGecko?

CoinGecko provides an external, realistic cryptocurrency market-data source.

Using a real API makes the project closer to a real-world ingestion workload than using manually generated sample data.

---

## Why Kafka?

Kafka introduces an event-streaming layer between ingestion and downstream processing.

Instead of:

```text
API → Database
```

the architecture becomes:

```text
API
 ↓
Kafka
 ↓
Consumer
 ↓
Data Lake
 ↓
Warehouse
```

This demonstrates concepts required for streaming-oriented data engineering systems.

The current implementation uses Kafka locally and orchestrates the complete workflow hourly through Airflow.

---

## Why a Local Data Lake?

A local data lake keeps the project zero-cost while demonstrating:

* Raw storage
* Processed storage
* Data layers
* Partitioning
* Parquet
* Data-lake organization

The architecture can later be migrated to cloud object storage.

---

## Why Parquet?

Parquet is a columnar storage format suited to analytical workloads.

Compared with raw JSON, Parquet provides a more structured and analytics-friendly representation of processed data.

---

## Why BigQuery?

BigQuery provides a cloud analytical warehouse without requiring the project to maintain a database server.

The project uses the BigQuery Sandbox/zero-cost development approach.

---

## Why dbt?

dbt separates warehouse transformation logic from Python ingestion code.

It provides:

* SQL models
* Data-quality tests
* Freshness checks
* Deduplication
* Reusable transformations
* Analytical marts

---

## Why Airflow?

Airflow coordinates the complete workflow and makes dependencies explicit:

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
dbt
```

Airflow also provides scheduling and task-level execution visibility.

---

## Why Terraform?

Terraform makes cloud infrastructure reproducible and version-controlled.

Instead of manually creating BigQuery infrastructure:

```text
Terraform configuration
        ↓
Terraform plan
        ↓
Terraform apply
        ↓
BigQuery resources
```

Terraform and dbt are intentionally separated so infrastructure and analytical transformations do not compete for ownership of the same resources.

---

## Why GitHub Actions?

GitHub Actions automatically validates changes to the repository.

The CI pipeline checks:

```text
Python
   ↓
Compile + Tests

Terraform
   ↓
Format + Validate

dbt
   ↓
Parse
```

This reduces the risk of introducing broken application code, invalid infrastructure configuration, or malformed dbt projects.

---

## Why Streamlit?

Streamlit provides a lightweight analytical interface for exposing the processed warehouse data.

It allows the project to demonstrate the complete path from:

```text
Data ingestion
      ↓
Data engineering
      ↓
Data warehouse
      ↓
Analytics
      ↓
User-facing dashboard
```

---

# 29. Project Challenges and Solutions

## Challenge 1 — BigQuery Timestamp Compatibility

### Problem

Parquet timestamps generated with nanosecond precision were incompatible with BigQuery.

### Solution

The pipeline converted timestamps to UTC and stored them using microsecond precision.

```text
Pandas
   ↓
UTC timestamp
   ↓
Microsecond precision
   ↓
Parquet
   ↓
BigQuery
```

---

## Challenge 2 — Kafka Invalid Events

### Problem

Invalid events should not stop the entire processing pipeline.

### Solution

A Dead Letter Queue was implemented:

```text
Valid Event
    ↓
Normal Processing

Invalid Event
    ↓
DLQ
```

---

## Challenge 3 — Infrastructure Ownership

### Problem

Terraform and dbt should not attempt to manage the same BigQuery resources.

### Solution

Responsibilities were separated:

```text
Terraform
    ↓
Infrastructure

dbt
    ↓
Transformation + Analytics
```

---

## Challenge 4 — API Authentication

### Problem

CoinGecko API requests require the appropriate authentication configuration for the project's usage.

### Solution

The project loads the API key from a local `.env` file through the application configuration layer.

The secret is excluded from Git version control.

---

## Challenge 5 — Multiple Airflow Projects

### Problem

The machine contains more than one Airflow project.

### Solution

CryptoData uses its own Airflow home:

```text
~/Desktop/CryptoData/airflow
```

This prevents its metadata database and logs from interfering with other Airflow projects.

---

# 30. Current Project Status

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
Hourly scheduling          ✅
Docker                     ✅
Terraform                  ✅
CI/CD                      ✅
Python unit tests          ✅
Dashboard                  ✅
Pipeline monitoring        ✅
End-to-end execution      ✅
Git repository cleanup     ✅
```

Current validation:

```text
Airflow tasks:        6/6 successful
Python tests:         9/9 successful
dbt tests:           17/17 successful
Terraform checks:     PASS
GitHub Actions CI:    SUCCESS
Dashboard:            COMPLETE
Hourly schedule:      VERIFIED
Git repository:       CLEAN
```

---

# 31. Future Improvements

The core portfolio platform is complete.

Future improvements could include:

## Cloud Data Lake

Replace or complement the local data lake with:

```text
Google Cloud Storage
```

while preserving the existing raw/processed architecture.

## Security

Add:

* Dedicated service accounts
* More granular IAM roles
* Google Secret Manager
* Least-privilege access
* Additional credential isolation

## Advanced Streaming

Extend Kafka with:

* More cryptocurrency assets
* Continuous producers
* Multiple consumer groups
* Stream processing
* Event-time processing
* Higher-volume workloads

## Advanced Monitoring

Add:

* Airflow alerting
* Kafka metrics
* Pipeline metrics
* Centralized logging
* Failure notifications
* Data-quality alerting

## Production Deployment

Extend CI/CD toward automated deployment of:

* Cloud infrastructure
* Airflow components
* dbt models
* Dashboard
* Supporting services

These are future production-oriented extensions rather than missing components of the current portfolio implementation.

---

# 32. Project Outcome

The Cryptocurrency Market Data Platform demonstrates a complete modern data engineering workflow:

```text
                    CoinGecko API
                          │
                          ▼
                    Python Ingestion
                          │
                          ▼
                       Kafka
                          │
                          ▼
                  Kafka Consumer
                          │
                 ┌────────┴────────┐
                 ▼                 ▼
             Data Lake             DLQ
                 │
                 ▼
              Parquet
                 │
                 ▼
             BigQuery
                 │
                 ▼
                dbt
                 │
                 ▼
          Analytical Marts
                 │
                 ▼
          Streamlit Dashboard
```

The complete workflow is orchestrated by:

```text
Apache Airflow
```

and infrastructure is managed through:

```text
Terraform
```

while repository changes are validated through:

```text
GitHub Actions
```

The platform has been successfully validated with:

```text
6/6 Airflow tasks successful
9/9 Python tests passing
17/17 dbt tests passing
Terraform validation passing
GitHub Actions CI passing
Hourly Airflow scheduling verified
Dashboard completed
Git repository clean
```

The project demonstrates practical experience with:

* API ingestion
* REST APIs
* Event streaming
* Apache Kafka
* Kafka producers and consumers
* Dead Letter Queues
* Data lakes
* JSON
* Parquet
* Pandas
* PyArrow
* Google BigQuery
* SQL transformation
* dbt
* Data quality
* Apache Airflow
* Workflow scheduling
* Docker
* Terraform
* Infrastructure as Code
* Git/GitHub
* GitHub Actions
* CI/CD
* Streamlit
* Plotly
* Analytical dashboards

The result is a complete portfolio-oriented data engineering platform covering the path from **external data source → ingestion → streaming → storage → transformation → warehouse → orchestration → infrastructure → CI/CD → analytics**.
