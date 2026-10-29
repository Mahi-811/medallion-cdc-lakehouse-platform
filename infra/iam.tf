# -------------------------------------------------------------
# Service Account: Ingestion Pipelines (Least Privilege)
# -------------------------------------------------------------
resource "google_service_account" "sa_ingestion" {
  account_id   = "sa-data-ingestion"
  display_name = "Service Account for Ingestion Pipelines (API, CSV, CDC)"
  description  = "Grants write access to Bronze BigQuery dataset and GCS landing buckets."
}

# Ingestion: Object Admin on landing and quarantine buckets
resource "google_storage_bucket_iam_member" "ingestion_csv_landing" {
  bucket = google_storage_bucket.csv_landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.sa_ingestion.email}"
}

resource "google_storage_bucket_iam_member" "ingestion_csv_quarantine" {
  bucket = google_storage_bucket.csv_quarantine.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.sa_ingestion.email}"
}

# Ingestion: Data Editor on Bronze BigQuery Dataset only
resource "google_bigquery_dataset_access" "ingestion_bronze_access" {
  dataset_id    = google_bigquery_dataset.bronze.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = google_service_account.sa_ingestion.email
}

# -------------------------------------------------------------
# Service Account: dbt Transformation Runner (Least Privilege)
# -------------------------------------------------------------
resource "google_service_account" "sa_dbt" {
  account_id   = "sa-data-dbt-runner"
  display_name = "Service Account for dbt Medallion Transformations"
  description  = "Reads Bronze, transforms and writes into Silver, Gold, and Marts."
}

# dbt: BigQuery Job User (required to submit query jobs)
resource "google_project_iam_member" "dbt_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa_dbt.email}"
}

# dbt: Viewer on Bronze (read-only)
resource "google_bigquery_dataset_access" "dbt_bronze_read" {
  dataset_id    = google_bigquery_dataset.bronze.dataset_id
  role          = "roles/bigquery.dataViewer"
  user_by_email = google_service_account.sa_dbt.email
}

# dbt: Editor on Silver, Gold, and Marts
resource "google_bigquery_dataset_access" "dbt_silver_editor" {
  dataset_id    = google_bigquery_dataset.silver.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = google_service_account.sa_dbt.email
}

resource "google_bigquery_dataset_access" "dbt_gold_editor" {
  dataset_id    = google_bigquery_dataset.gold.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = google_service_account.sa_dbt.email
}

resource "google_bigquery_dataset_access" "dbt_marts_editor" {
  dataset_id    = google_bigquery_dataset.marts.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = google_service_account.sa_dbt.email
}
