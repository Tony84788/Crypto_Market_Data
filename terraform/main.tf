provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "crypto_market" {
  dataset_id = var.dataset_id
  location   = "US"

  default_partition_expiration_ms = 5184000000
  default_table_expiration_ms     = 5184000000
}

resource "google_bigquery_table" "crypto_prices" {
  dataset_id = google_bigquery_dataset.crypto_market.dataset_id
  table_id   = "crypto_prices"

  expiration_time = 1795610938708

  schema = <<EOF
[
  {
    "name": "event_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "event_type",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "source",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "received_at",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "symbol",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "current_price",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "market_cap",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "market_cap_rank",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "total_volume",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "high_24h",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "low_24h",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "price_change_24h",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "price_change_percentage_24h",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "circulating_supply",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "total_supply",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "max_supply",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "ath",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "atl",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "last_updated",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  }
]
EOF
}
