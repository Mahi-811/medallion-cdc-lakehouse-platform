# BigQuery Bronze Dataset (Raw Ingestion)
resource "google_bigquery_dataset" "bronze" {
  dataset_id                  = "bronze"
  friendly_name               = "Bronze Raw Lakehouse"
  description                 = "Append-only landing layer containing raw CDC events, CSV drops, and API metrics."
  location                    = var.region
  default_table_expiration_ms = null

  labels = {
    layer       = "bronze"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# BigQuery Silver Dataset (Cleaned & Conformed)
resource "google_bigquery_dataset" "silver" {
  dataset_id                  = "silver"
  friendly_name               = "Silver Conformed"
  description                 = "Cleaned, typed, deduplicated layer with CDC deletes & updates resolved."
  location                    = var.region
  default_table_expiration_ms = null

  labels = {
    layer       = "silver"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# BigQuery Gold Dataset (Star Schema Dimensions & Facts)
resource "google_bigquery_dataset" "gold" {
  dataset_id                  = "gold"
  friendly_name               = "Gold Dimensional Star Schema"
  description                 = "Dimensional star schema (fact_orders, dim_customer SCD2, dim_product, dim_date)."
  location                    = var.region
  default_table_expiration_ms = null

  labels = {
    layer       = "gold"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# BigQuery Marts Dataset (Business Rollups & KPIs)
resource "google_bigquery_dataset" "marts" {
  dataset_id                  = "marts"
  friendly_name               = "Analytical Business Marts"
  description                 = "Executive and operational business marts (daily revenue, customer LTV, product velocity)."
  location                    = var.region
  default_table_expiration_ms = null

  labels = {
    layer       = "marts"
    environment = var.environment
    managed_by  = "terraform"
  }
}
