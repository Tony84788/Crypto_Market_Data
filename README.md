# ₿ Cryptocurrency Market Data Platform

An end-to-end data engineering platform that collects real-time cryptocurrency market data from the CoinGecko API, streams it through Apache Kafka, processes it into a local data lake, loads it into Google BigQuery, transforms it with dbt, orchestrates the complete workflow with Apache Airflow, and presents the resulting data through an interactive Streamlit dashboard.

The project is designed as a practical data engineering portfolio demonstrating API ingestion, streaming, data lakes, Parquet, cloud warehousing, SQL transformation, orchestration, infrastructure as code, data quality, monitoring, CI/CD, and analytics.

---

## 📌 Project Overview

Cryptocurrency market data changes continuously.

This project builds a pipeline capable of repeatedly collecting cryptocurrency market information and moving it through several stages:

```text
CoinGecko API
      │
      ▼
Python Ingestion
      │
      ▼
Apache Kafka
      │
      ├──────────────► Dead Letter Queue
      │
      ▼
Kafka Consumer
      │
      ▼
Local Data Lake
      │
      ▼
Parquet
      │
      ▼
Google BigQuery
      │
      ▼
dbt Transformations
      │
      ▼
Analytical Marts
      │
      ▼
Streamlit Dashboard
```

Apache Airflow orchestrates the complete workflow.

```text
                    Apache Airflow
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   Extraction        Streaming       Processing
        │                │                │
        └────────────────┼────────────────┘
                         ▼
                    BigQuery
                         │
                         ▼
                       dbt
                         │
                         ▼
                     Dashboard
```

---

# 🎯 Project Objectives

The platform was designed to demonstrate the following data engineering capabilities:

* REST API ingestion
* Python data pipelines
* Streaming with Apache Kafka
* Kafka topic management
* Dead Letter Queue handling
* Event-based processing
* Data lake architecture
* JSON and Parquet processing
* Pandas transformations
* Data validation
* Data freshness monitoring
* Google BigQuery
* dbt transformations
* dbt testing
* Airflow orchestration
* Scheduled pipelines
* Docker
* Terraform
* CI/CD with GitHub Actions
* Streamlit dashboards
* Plotly visualizations
* Cloud authentication
* Infrastructure as Code
* Git and GitHub
* Data quality engineering

---

# 🌐 Data Source

## CoinGecko

The platform uses the CoinGecko cryptocurrency market API as its external data source.

Official documentation:

[CoinGecko API Documentation](https://docs.coingecko.com/?utm_source=chatgpt.com)

Primary endpoint:

```text
https://api.coingecko.com/api/v3/coins/markets
```

The API provides cryptocurrency market information such as:

* Cryptocurrency ID
* Symbol
* Name
* Current price
* Market capitalization
* Market-cap rank
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

The current pipeline retrieves:

```text
bitcoin
ethereum
solana
```

---

# 🏗️ Architecture

```text
                         ┌───────────────────┐
                         │   CoinGecko API   │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ Python Ingestion  │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │   Apache Kafka    │
                         │ crypto-market-data│
                         └─────────┬─────────┘
                                   │
                     ┌─────────────┴─────────────┐
                     │                           │
                     ▼                           ▼
              ┌─────────────┐            ┌─────────────┐
              │Kafka Consumer│            │     DLQ     │
              └──────┬──────┘            └─────────────┘
                     │
                     ▼
              ┌─────────────┐
              │ Data Lake   │
              │ JSON/Parquet│
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │  BigQuery   │
              │ crypto_market│
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │     dbt     │
              │ Staging +   │
              │    Marts    │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │ Streamlit   │
              │ Dashboard   │
              └─────────────┘
```

Airflow controls the execution order:

```text
extract
   ↓
produce
   ↓
consume
   ↓
transform
   ↓
load
   ↓
dbt
```

---

# 🧰 Technology Stack

| Layer                  | Technology                             |
| ---------------------- | -------------------------------------- |
| Data Source            | CoinGecko API                          |
| Programming            | Python                                 |
| Data Processing        | Pandas                                 |
| Streaming              | Apache Kafka                           |
| Messaging              | Kafka 4.1.0                            |
| Containerization       | Docker                                 |
| Data Lake              | Local filesystem                       |
| File Format            | JSON / Parquet                         |
| Cloud Warehouse        | Google BigQuery                        |
| Transformation         | dbt                                    |
| Orchestration          | Apache Airflow 3.3.2                   |
| Infrastructure as Code | Terraform                              |
| Dashboard              | Streamlit                              |
| Visualization          | Plotly                                 |
| CI/CD                  | GitHub Actions                         |
| Version Control        | Git / GitHub                           |
| Authentication         | Google Application Default Credentials |

The project was implemented with free or locally available components where possible.

---

# 🔄 Data Pipeline

## 1. Extraction

Python connects to the CoinGecko API and retrieves cryptocurrency market data.

Example:

```text
bitcoin
ethereum
solana
```

The extraction process also performs a basic monitoring check.

```python
record_count = len(crypto_data)

if record_count == 0:
    raise ValueError(
        "Monitoring check failed: CoinGecko returned 0 records."
    )
```

This prevents the pipeline from silently continuing when the API returns no records.

---

# 2. Kafka Streaming

The extracted records are published to Apache Kafka.

Main Kafka topic:

```text
crypto-market-data
```

Configuration:

```text
Partitions: 3
Replication factor: 1
Min ISR: 1
```

A separate Dead Letter Queue topic is also configured:

```text
crypto-market-data-dlq
```

The DLQ provides a place for messages that cannot be processed successfully.

---

# 3. Kafka Event Envelope

Events are wrapped with metadata before being written to the data lake.

Conceptually:

```json
{
  "event_id": "...",
  "event_type": "crypto_market_data",
  "received_at": "...",
  "source": "coingecko",
  "data": {
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin"
  }
}
```

This separates event metadata from the actual cryptocurrency payload.

---

# 4. Data Lake

Kafka-consumed events are persisted into the local data lake.

Structure:

```text
data/
└── lake/
    ├── raw/
    │   └── crypto_market/
    │
    └── processed/
        └── crypto_market/
```

The raw layer preserves incoming events.

The processed layer contains cleaned and transformed Parquet data.

---

# 5. Parquet Transformation

Pandas is used to transform the raw events.

Parquet was selected because it provides:

* Columnar storage
* Efficient analytical reads
* Compression
* Schema preservation
* Better analytical performance than raw JSON

The processed data is stored as Parquet files.

---

# 6. Timestamp Handling

One of the BigQuery loading challenges was timestamp compatibility.

The pipeline normalizes timestamps before loading them into BigQuery.

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

This ensures that the timestamps can be loaded consistently into BigQuery.

---

# 7. Data Validation

The pipeline performs multiple validation checks.

Validation includes:

* Required columns
* Required values
* Numeric ranges
* Duplicate cryptocurrency IDs
* Timestamp validity
* Timestamp freshness

Freshness validation checks whether the source data was already too old when it entered the pipeline.

The current freshness threshold is:

```text
30 minutes
```

Records older than the configured threshold are treated as stale.

---

# ☁️ Google BigQuery

The processed Parquet data is loaded into Google BigQuery.

Project:

```text
crypto-market-data-platform
```

Dataset:

```text
crypto_market
```

Location:

```text
US
```

The dataset uses a default 60-day expiration configuration for managed resources.

---

# 🗄️ BigQuery Tables and Models

The main raw table is:

```text
crypto_prices
```

dbt creates analytical models including:

```text
stg_crypto_prices
fct_crypto_price_history
mart_crypto_latest
```

Conceptually:

```text
crypto_prices
      │
      ▼
stg_crypto_prices
      │
      ├──────────────► mart_crypto_latest
      │
      └──────────────► fct_crypto_price_history
```

---

# 🧪 dbt

dbt is used to transform and test the BigQuery data.

The dbt project contains:

```text
crypto_dbt/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   └── stg_crypto_prices.sql
│   │
│   └── marts/
│       ├── fct_crypto_price_history.sql
│       └── mart_crypto_latest.sql
│
└── tests/
```

The historical model filters invalid records:

```sql
SELECT
    crypto_id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    circulating_supply,
    total_supply,
    max_supply,
    ath,
    atl,
    last_updated
FROM {{ ref('stg_crypto_prices') }}
WHERE current_price IS NOT NULL
  AND current_price > 0
  AND last_updated IS NOT NULL
```

dbt testing successfully passed:

```text
17/17 tests passed
```

---

# ⏰ Airflow Orchestration

Apache Airflow orchestrates the complete cryptocurrency pipeline.

Airflow version:

```text
3.3.2
```

DAG:

```text
crypto_market_pipeline
```

Schedule:

```text
0 * * * *
```

This means:

```text
Every hour
```

The DAG contains six tasks:

```text
extract_from_coingecko
          ↓
produce_to_kafka
          ↓
consume_from_kafka
          ↓
transform_to_parquet
          ↓
load_to_bigquery
          ↓
run_dbt
```

---

# 📊 Airflow Pipeline Monitoring

The extraction task checks the number of records returned by CoinGecko.

Example:

```text
Retrieved 3 cryptocurrencies from CoinGecko.
Monitoring check passed: 3 records retrieved.
```

The pipeline also performs freshness validation during data processing.

This provides two levels of protection:

```text
API availability
      +
Data freshness
      +
Schema/data validation
```

---

# 🐳 Docker

Docker is used to run Apache Kafka locally.

Kafka container:

```text
crypto-kafka
```

Image:

```text
apache/kafka:4.1.0
```

Port:

```text
9092
```

Docker Compose is used to manage the Kafka service.

---

# 🏗️ Terraform

Terraform manages the BigQuery infrastructure used by the project.

Terraform version:

```text
1.16.4
```

Google provider:

```text
7.46.1
```

Terraform manages:

```text
BigQuery dataset
BigQuery raw table
Table configuration
Expiration configuration
```

Terraform does **not** create the entire Google Cloud project.

The existing GCP project is:

```text
crypto-market-data-platform
```

Terraform manages resources inside that project.

Terraform validation completed successfully with:

```text
No changes
0 added
0 changed
0 destroyed
```

---

# 🔐 Security

Sensitive credentials are not stored in the repository.

Local environment variables are stored in:

```text
.env
```

Example:

```text
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
COINGECKO_API_KEY=
```

The actual API key is kept locally and is excluded from Git.

The `.gitignore` contains:

```text
.env
.env.*
```

Google Cloud authentication uses Application Default Credentials rather than storing service-account credentials inside the repository.

---

# 🚀 CI/CD

GitHub Actions automatically performs project checks.

The CI pipeline contains three jobs:

```text
Python checks
      │
      ├── Compile source
      └── Run pytest

Terraform checks
      │
      ├── terraform fmt
      ├── terraform init
      └── terraform validate

dbt checks
      │
      └── dbt parse
```

The workflow runs on:

```text
push → master
pull_request → master
```

Latest CI validation completed successfully.

---

# 📈 Streamlit Dashboard

The project includes an interactive Streamlit dashboard.

Dashboard technologies:

```text
Streamlit
Plotly
BigQuery
```

The dashboard provides:

### Data freshness

```text
Fresh       ≤ 10 minutes
Aging       10–30 minutes
Stale       > 30 minutes
```

### Market overview

The dashboard displays:

* Current cryptocurrency prices
* Market capitalization
* 24-hour trading volume
* 24-hour price performance
* Historical prices
* Latest available records

Users can select cryptocurrencies for historical analysis.

The dashboard also includes:

* Refresh Data
* Missing-data handling
* BigQuery error handling
* Partial-data handling
* Responsive layout
* Sidebar controls
* Human-readable currency formatting

---

# 📁 Project Structure

```text
CryptoData/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── airflow/
│   └── dags/
│       └── crypto_market_pipeline.py
│
├── crypto_dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   └── stg_crypto_prices.sql
│       └── marts/
│           ├── fct_crypto_price_history.sql
│           └── mart_crypto_latest.sql
│
├── dashboard/
│   └── app.py
│
├── data/
│   └── lake/
│       ├── raw/
│       │   └── crypto_market/
│       └── processed/
│           └── crypto_market/
│
├── src/
│   ├── bigquery/
│   ├── config/
│   ├── ingestion/
│   ├── kafka/
│   └── lake/
│
├── terraform/
│   ├── main.tf
│   ├── outputs.tf
│   ├── variables.tf
│   └── versions.tf
│
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
└── requirements-dev.txt
```

---

# ⚙️ Environment Setup

The project uses a Conda environment:

```bash
conda create -n crypto_market python=3.12 -y
```

Activate it:

```bash
conda activate crypto_market
```

Install project dependencies:

```bash
pip install -r requirements.txt
```

Development dependencies:

```bash
pip install -r requirements-dev.txt
```

---

# 🔑 Environment Variables

Create the local `.env` file:

```bash
touch .env
```

Configure:

```text
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
COINGECKO_API_KEY=your_api_key
```

The real API key should never be committed to Git.

---

# ▶️ Running the Pipeline

## Run Python ingestion

```bash
python -m src.ingestion.main
```

---

## Start Kafka

Check Docker:

```bash
docker ps
```

Check Kafka:

```bash
docker ps | grep crypto-kafka
```

---

## Create/check Kafka topics

Main topic:

```text
crypto-market-data
```

DLQ:

```text
crypto-market-data-dlq
```

---

# 🛫 Running Airflow

Set the project-specific Airflow home:

```bash
export AIRFLOW_HOME="$HOME/Desktop/CryptoData/airflow"
```

Start Airflow:

```bash
airflow standalone
```

The CryptoData project uses its own Airflow environment.

The separate Weather_ETL Airflow environment remains independent.

---

# 🔍 Testing the Pipeline

Compile Python source:

```bash
python -m compileall src
```

Run tests:

```bash
pytest
```

Run dbt:

```bash
cd crypto_dbt
dbt debug
dbt build
```

Validate Terraform:

```bash
cd terraform
terraform fmt -check
terraform init -backend=false
terraform validate
```

---

# 📊 Pipeline Execution Example

A successful execution follows:

```text
1. CoinGecko API
       ↓
2. Extract cryptocurrency records
       ↓
3. Publish records to Kafka
       ↓
4. Consume Kafka messages
       ↓
5. Store raw events
       ↓
6. Transform to Parquet
       ↓
7. Load Parquet into BigQuery
       ↓
8. Execute dbt
       ↓
9. Build analytical models
       ↓
10. Dashboard reads BigQuery
```

A successful Airflow execution has completed all six tasks:

```text
extract_from_coingecko       SUCCESS
produce_to_kafka             SUCCESS
consume_from_kafka           SUCCESS
transform_to_parquet         SUCCESS
load_to_bigquery             SUCCESS
run_dbt                      SUCCESS
```

Both manual and scheduled hourly executions have been successfully verified.

---

# 🧠 Engineering Concepts Demonstrated

## Batch processing

CoinGecko market records are collected and transformed as datasets.

## Streaming

Kafka provides the event-streaming layer between extraction and downstream processing.

## Data lake

Raw and processed datasets are persisted locally.

## Columnar storage

Parquet provides an analytical storage format.

## Data warehouse

BigQuery provides cloud analytical storage.

## ELT

dbt performs transformations inside BigQuery.

## Orchestration

Airflow controls dependencies, execution order, and scheduling.

## Infrastructure as Code

Terraform defines BigQuery infrastructure declaratively.

## Data quality

Validation checks protect against:

* Empty API responses
* Invalid values
* Duplicate IDs
* Invalid timestamps
* Stale data

## Monitoring

The pipeline monitors:

* Record counts
* Data freshness
* Task success/failure

## CI/CD

GitHub Actions automatically validates:

* Python
* Terraform
* dbt

## Analytics

Streamlit and Plotly provide a user-facing analytical interface.

---

# 🛠️ Important Design Decisions

### Why CoinGecko?

It provides real cryptocurrency market data through a public API and is suitable for demonstrating API-based data ingestion.

### Why Kafka?

Kafka introduces a real streaming component instead of connecting the API directly to the warehouse.

### Why Parquet?

Parquet is optimized for analytical workloads and provides efficient columnar storage.

### Why BigQuery?

BigQuery provides a scalable cloud data warehouse suitable for analytical queries.

### Why dbt?

dbt separates warehouse transformations from ingestion and provides testing and model dependency management.

### Why Airflow?

Airflow provides scheduling, dependency management, retries, task monitoring, and workflow orchestration.

### Why Terraform?

Terraform makes infrastructure reproducible and version-controlled.

### Why Streamlit?

Streamlit provides a simple way to expose the analytical results through an interactive dashboard.

---

# 🧩 Challenges and Solutions

## Challenge 1 — API authentication

The API key was initially not being loaded correctly by the application.

### Solution

The settings module was configured to explicitly load the project-root `.env` file.

---

## Challenge 2 — Kafka consumer termination

A streaming consumer can wait indefinitely for messages.

### Solution

The pipeline uses a bounded consumer that processes the expected Kafka messages and exits.

---

## Challenge 3 — Failed Kafka messages

Not every event should block the pipeline.

### Solution

A Dead Letter Queue topic was introduced:

```text
crypto-market-data-dlq
```

---

## Challenge 4 — Timestamp compatibility

Timezone-aware timestamps caused compatibility issues when loading into BigQuery.

### Solution

Timestamps were normalized to UTC and converted to BigQuery-compatible microsecond precision.

---

## Challenge 5 — Data freshness

An API response can succeed while still containing old market data.

### Solution

Timestamp freshness validation was added to detect stale records.

---

## Challenge 6 — Infrastructure drift

Manually created BigQuery resources can differ from infrastructure definitions.

### Solution

Terraform was used to import and manage the existing BigQuery resources.

Terraform subsequently confirmed:

```text
No changes
```

---

# 📋 Current Project Status

| Component                 | Status     |
| ------------------------- | ---------- |
| CoinGecko API             | ✅ Complete |
| Python ingestion          | ✅ Complete |
| Raw JSON                  | ✅ Complete |
| Apache Kafka              | ✅ Complete |
| Kafka producer            | ✅ Complete |
| Kafka consumer            | ✅ Complete |
| Kafka DLQ                 | ✅ Complete |
| Local data lake           | ✅ Complete |
| Pandas transformation     | ✅ Complete |
| Parquet                   | ✅ Complete |
| Data validation           | ✅ Complete |
| Data freshness monitoring | ✅ Complete |
| BigQuery                  | ✅ Complete |
| dbt staging               | ✅ Complete |
| dbt deduplication         | ✅ Complete |
| dbt freshness             | ✅ Complete |
| dbt tests                 | ✅ Complete |
| dbt marts                 | ✅ Complete |
| Airflow DAG               | ✅ Complete |
| Airflow orchestration     | ✅ Complete |
| Hourly scheduling         | ✅ Complete |
| Docker                    | ✅ Complete |
| Terraform                 | ✅ Complete |
| CI/CD                     | ✅ Complete |
| Streamlit dashboard       | ✅ Complete |
| GitHub repository         | ✅ Complete |

---

# 🚀 Future Improvements

Possible future extensions include:

* Add more cryptocurrencies
* Increase API collection frequency where appropriate
* Add additional market endpoints
* Introduce Kafka Schema Registry
* Add stronger event schemas
* Add automated alerting
* Add more advanced anomaly detection
* Add Spark processing
* Add cloud object storage
* Add partitioned BigQuery historical tables
* Add more dbt analytical marts
* Add authentication to the dashboard
* Add automated deployment
* Add infrastructure monitoring
* Add more comprehensive integration tests

These are potential extensions rather than requirements for the current completed implementation.

---

# 📌 Project Outcome

This project demonstrates how a modern data engineering platform can move data through multiple stages:

```text
External API
     ↓
Ingestion
     ↓
Streaming
     ↓
Data Lake
     ↓
Columnar Storage
     ↓
Cloud Warehouse
     ↓
Transformation
     ↓
Data Quality
     ↓
Orchestration
     ↓
Analytics
```

It combines both **batch and streaming concepts** into one end-to-end platform while demonstrating practical engineering practices such as:

* modular Python development
* API authentication
* event streaming
* fault handling
* data validation
* freshness monitoring
* analytical storage
* SQL transformation
* workflow orchestration
* infrastructure as code
* automated testing
* CI/CD
* visualization

The result is a complete cryptocurrency data engineering platform suitable for demonstrating practical data engineering skills in a portfolio.

---

# 👨‍💻 Author

**Anthony Madu**

Computer Engineering Student
Data Engineering / Data Platform Development

GitHub:

[Tony84788 GitHub](https://github.com/Tony84788)

Project repository:

[Crypto Market Data Platform](https://github.com/Tony84788/Crypto_Market_Data)

---

# 📄 License

This project is intended for educational and portfolio purposes.
