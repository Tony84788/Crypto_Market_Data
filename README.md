# Cryptocurrency Market Data Platform

An end-to-end cryptocurrency market data engineering platform that extracts real-time market data from the CoinGecko API, streams it through Apache Kafka, stores raw and processed data in a local data lake, loads analytical data into Google BigQuery, transforms it with dbt, and orchestrates the complete workflow with Apache Airflow.

The project is designed as a zero-cost learning and portfolio project using local infrastructure, Docker, Kafka, and the BigQuery Sandbox.

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
                    │ Processed Parquet             │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │        Google BigQuery       │
                    │                              │
                    │ crypto_prices                 │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │             dbt              │
                    │                              │
                    │ staging                      │
                    │ deduplication                │
                    │ validation/testing            │
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
4. Store raw events in a local data lake.
5. Transform raw events using Pandas.
6. Store processed data as Parquet.
7. Load Parquet data into Google BigQuery.
8. Transform and test warehouse data using dbt.
9. Create analytical marts.
10. Orchestrate the entire pipeline using Apache Airflow.

The pipeline processes cryptocurrencies including:

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

---

# 3. Technology Stack

| Technology       | Purpose                        |
| ---------------- | ------------------------------ |
| Python           | Data ingestion and processing  |
| Requests         | CoinGecko API communication    |
| Pandas           | Data transformation            |
| PyArrow          | Parquet processing             |
| Apache Kafka     | Real-time event streaming      |
| Docker           | Kafka container infrastructure |
| Local Data Lake  | Raw and processed storage      |
| Parquet          | Columnar data format           |
| Google BigQuery  | Cloud data warehouse           |
| dbt              | SQL transformation and testing |
| Apache Airflow   | Workflow orchestration         |
| Conda            | Python environment management  |
| Google Cloud SDK | GCP interaction                |
| Git/GitHub       | Version control                |

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
    ├── tests
    └── marts
```

---

# 5. Kafka Architecture

Kafka is used to introduce a streaming layer between data extraction and downstream processing.

## Kafka Topic

```text
crypto-market-data
```

The topic uses:

```text
3 partitions
Replication factor: 1
```

A separate topic is used for rejected events:

```text
crypto-market-data-dlq
```

The Dead Letter Queue allows invalid events to be isolated instead of stopping the entire pipeline.

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

This separates event metadata from the actual cryptocurrency payload.

---

# 7. Data Lake

The project uses a local data lake to maintain a zero-cost development environment.

Directory structure:

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

## Raw layer

Raw Kafka events are stored as JSON.

```text
data/lake/raw/crypto_market/
```

## Processed layer

Processed records are stored as Parquet.

```text
data/lake/processed/crypto_market/
```

The processed data uses date-based partitioning:

```text
year=2026/
month=09/
day=27/
```

---

# 8. Parquet Timestamp Handling

The pipeline initially encountered a BigQuery compatibility problem because Pandas/PyArrow generated Parquet timestamps using nanosecond precision.

BigQuery reported:

```text
Invalid timestamp nanoseconds value
of logical type TIMESTAMP_NANOS
```

The transformation layer was updated to:

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

This converts timestamps to UTC, removes timezone metadata from the Parquet representation, and stores them using microsecond precision.

The resulting Parquet files load successfully into BigQuery.

---

# 9. BigQuery

Google BigQuery is used as the analytical warehouse.

## GCP Project

```text
crypto-market-data-platform
```

## Dataset

```text
crypto_market
```

## Main table

```text
crypto_prices
```

The table contains fields including:

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

---

# 10. dbt

dbt provides the SQL transformation and data-quality layer.

Project:

```text
crypto_dbt/
```

The dbt pipeline performs:

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

## dbt tests

The project contains automated data-quality tests covering:

* Uniqueness
* Valid values
* Cryptocurrency records
* Staging data
* Mart data

The project previously achieved:

```text
17/17 dbt tests passing
```

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

This allows the pipeline to be triggered manually during development.

---

# 12. Airflow Execution

The pipeline was successfully executed end-to-end.

Successful run:

```text
Run ID:
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

The final workflow was:

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

This confirms that the complete pipeline works from external API ingestion through warehouse transformation.

---

# 13. Project Structure

```text
CryptoData/
│
├── .env
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yml
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
├── data/
│   ├── raw/
│   ├── processed/
│   └── lake/
│
├── tests/
│   └── __init__.py
│
├── logs/
│
├── docs/
│
├── scripts/
│
├── airflow/
│   ├── airflow.db
│   ├── dags/
│   │   └── crypto_market_pipeline.py
│   ├── logs/
│   └── plugins/
│
└── crypto_dbt/
    ├── dbt_project.yml
    ├── models/
    ├── tests/
    └── ...
```

---

# 14. Environment Setup

Create and activate the Conda environment:

```bash
conda create -n crypto_market python=3.12
conda activate crypto_market
```

Install the required packages.

The project uses Conda where practical to reduce binary compatibility issues between:

* NumPy
* Pandas
* PyArrow

Verify the environment:

```bash
python --version
```

Expected Python version:

```text
Python 3.12.x
```

---

# 15. Start Kafka

Kafka runs locally using Docker.

Check Docker:

```bash
sudo systemctl status docker
```

Start Kafka:

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

# 16. Test CoinGecko Extraction

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

# 17. Start Airflow

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

# 18. Trigger the Pipeline

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

# 19. Useful Validation Commands

## Check Kafka

```bash
docker ps --filter "name=crypto-kafka"
```

## Check Parquet files

```bash
find data/lake/processed/crypto_market -name "*.parquet"
```

## Check BigQuery rows

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

---

# 20. Data Quality

The pipeline implements multiple layers of data-quality control.

```text
CoinGecko API
     │
     ▼
Extraction validation
     │
     ▼
Kafka event validation
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

This prevents invalid records from silently propagating through the entire system.

---

# 21. Engineering Concepts Demonstrated

This project demonstrates practical knowledge of:

## Data ingestion

* REST APIs
* HTTP requests
* API parameters
* JSON
* API authentication
* Error handling

## Streaming

* Kafka producers
* Kafka consumers
* Topics
* Partitions
* Offsets
* Consumer groups
* Event envelopes
* Dead Letter Queues

## Data lakes

* Raw data
* Processed data
* JSON
* Parquet
* Partitioned storage

## Data transformation

* Pandas
* Timestamp handling
* Data validation
* Schema normalization

## Data warehousing

* Google BigQuery
* Datasets
* Tables
* Schema management
* Parquet ingestion

## Analytics engineering

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
* Google Cloud CLI

---

# 22. Key Design Decisions

### Why Kafka?

Kafka provides a streaming layer between ingestion and downstream processing.

Instead of:

```text
API → Database
```

the architecture becomes:

```text
API → Kafka → Consumers → Data Lake/Warehouse
```

This makes the architecture suitable for future real-time workloads.

### Why Parquet?

Parquet is a columnar format that provides efficient storage and analytical processing compared with raw JSON.

### Why BigQuery?

BigQuery provides a cloud-native analytical warehouse without requiring a locally managed database server.

### Why dbt?

dbt separates warehouse transformation logic from ingestion code and provides testing, documentation, freshness checks, and reusable SQL models.

### Why Airflow?

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
Transform/Test
```

---

# 23. Current Project Status

```text
CoinGecko API              ✅
Python ingestion           ✅
Raw JSON                   ✅
Kafka                      ✅
Kafka Producer             ✅
Kafka Consumer             ✅
Dead Letter Queue          ✅
Local Data Lake            ✅
Pandas transformation     ✅
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

Terraform                  ⬜
CI/CD                      ⬜
Dashboard                  ⬜
Additional documentation   ⬜
```

---

# 24. Future Improvements

The current implementation provides the core end-to-end pipeline. Future improvements could include:

### Infrastructure as Code

Add Terraform for:

* Google Cloud resources
* BigQuery datasets
* BigQuery tables
* IAM
* Service accounts
* Storage infrastructure

### CI/CD

Add GitHub Actions for:

```text
Git push
   ↓
Lint
   ↓
Unit tests
   ↓
dbt tests
   ↓
Build
   ↓
Deployment
```

### Scheduling

Change the Airflow DAG from:

```python
schedule=None
```

to a periodic schedule such as:

```text
Every 15 minutes
```

### Monitoring

Add:

* Airflow alerts
* Pipeline metrics
* Kafka monitoring
* Data-quality alerts
* Failure notifications

### Analytics

Add a dashboard showing:

* Cryptocurrency prices
* Market capitalization
* Trading volume
* 24-hour price changes
* Market rankings
* Historical trends

---

# 25. Project Outcome

The Cryptocurrency Market Data Platform successfully demonstrates a complete modern data engineering workflow:

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
```

The complete Airflow pipeline has been successfully executed with all six tasks passing:

```text
6/6 tasks successful
```

This project demonstrates practical experience with API ingestion, streaming data, data lakes, cloud warehousing, SQL transformation, data quality, orchestration, Docker, and cloud infrastructure.
