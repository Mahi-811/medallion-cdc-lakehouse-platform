output "bronze_raw_bucket_url" {
  description = "GCS URI for raw lakehouse storage"
  value       = google_storage_bucket.bronze_raw.url
}

output "csv_landing_bucket_url" {
  description = "GCS URI for CSV drop landings"
  value       = google_storage_bucket.csv_landing.url
}

output "csv_quarantine_bucket_url" {
  description = "GCS URI for quarantined CSV files"
  value       = google_storage_bucket.csv_quarantine.url
}

output "bigquery_datasets" {
  description = "IDs of the medallion BigQuery datasets"
  value = {
    bronze = google_bigquery_dataset.bronze.dataset_id
    silver = google_bigquery_dataset.silver.dataset_id
    gold   = google_bigquery_dataset.gold.dataset_id
    marts  = google_bigquery_dataset.marts.dataset_id
  }
}

output "ingestion_service_account_email" {
  description = "Email of the ingestion service account"
  value       = google_service_account.sa_ingestion.email
}

output "dbt_service_account_email" {
  description = "Email of the dbt transformation service account"
  value       = google_service_account.sa_dbt.email
}
