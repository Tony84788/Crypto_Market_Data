variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "crypto-market-data-platform"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "crypto_market"
}
