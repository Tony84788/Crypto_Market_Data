output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.crypto_market.dataset_id
}

output "bigquery_raw_table_id" {
  description = "BigQuery raw cryptocurrency table ID"
  value       = google_bigquery_table.crypto_prices.table_id
}

