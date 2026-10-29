variable "project_id" {
  description = "The Google Cloud Project ID to provision resources in."
  type        = string
  default     = "medallion-lakehouse-portfolio"
}

variable "region" {
  description = "The default GCP region for storage and BigQuery resources."
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "prod"
}

variable "retention_days_landing" {
  description = "Number of days before landed CSV files are automatically pruned by GCS lifecycle rules."
  type        = number
  default     = 30
}

variable "enable_versioning" {
  description = "Whether to enable object versioning on raw data buckets."
  type        = bool
  default     = true
}
