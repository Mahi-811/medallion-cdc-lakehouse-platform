terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.20.0"
    }
  }

  # Production state locking (uncomment when deploying to remote backend)
  # backend "gcs" {
  #   bucket = "medallion-lakehouse-tfstate"
  #   prefix = "terraform/state"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
