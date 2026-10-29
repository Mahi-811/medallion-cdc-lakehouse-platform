# GCS Bucket for Raw Bronze Lakehouse Storage
resource "google_storage_bucket" "bronze_raw" {
  name                        = "${var.project_id}-${var.environment}-bronze-lakehouse-raw"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = var.enable_versioning
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    layer       = "bronze"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# GCS Bucket for Arriving CSV Drops
resource "google_storage_bucket" "csv_landing" {
  name                        = "${var.project_id}-${var.environment}-csv-landing"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = var.retention_days_landing
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    purpose     = "csv-ingestion-landing"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# GCS Bucket for Quarantined Corrupt CSVs
resource "google_storage_bucket" "csv_quarantine" {
  name                        = "${var.project_id}-${var.environment}-csv-quarantine"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    purpose     = "csv-quarantine-dlq"
    environment = var.environment
    managed_by  = "terraform"
  }
}
