terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.28.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_path)
  project     = var.project_id
  region      = var.region
}

# Decode the credentials JSON and extract the client_email
locals {
  credentials           = jsondecode(file(var.credentials_path))
  service_account_email = local.credentials.client_email
}

# Call the iam_roles module
module "iam_roles" {
  source                = "./modules/iam_roles"
  project_id            = var.project_id
  service_account_email = local.service_account_email
}
###
resource "google_storage_bucket" "dataproc_staging_bucket" {
  name          = var.dataproc_staging_bucket
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "dataproc_temp_bucket" {
  name          = var.dataproc_temp_bucket
  location      = var.region
  force_destroy = true
}
resource "google_storage_bucket_iam_member" "staging_bucket_iam" {
  bucket = google_storage_bucket.dataproc_staging_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.service_account_email}"
}

resource "google_storage_bucket_iam_member" "temp_bucket_iam" {
  bucket = google_storage_bucket.dataproc_temp_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.service_account_email}"
}

##
resource "google_storage_bucket" "sales_data_bucket" {
  name          = var.bucket_name
  location      = var.bucket_location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.raw_dataset_id
  location                   = var.dataset_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id                 = var.marts_dataset_id
  location                   = var.dataset_location
  delete_contents_on_destroy = true
}

resource "google_dataproc_cluster" "sales_cluster" {
  name   = var.cluster_name
  region = var.cluster_region

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_staging_bucket.name
    temp_bucket    = google_storage_bucket.dataproc_temp_bucket.name
    gce_cluster_config {
      service_account = local.service_account_email
      zone            = var.cluster_zone
    }

    master_config {
      num_instances = 1
      machine_type  = var.cluster_machine_type
      disk_config {
        boot_disk_type    = var.boot_disk_type
        boot_disk_size_gb = 50
      }
    }

    lifecycle_config {
      idle_delete_ttl = var.auto_delete_ttl
    }

    worker_config {
      num_instances = 2
    }

    software_config {
      optional_components = ["JUPYTER"]
    }
    # You can define multiple initialization_action blocks
    initialization_action {
      script      = "gs://sales_data_bucket_project_cider/code/initialization_action.sh"
      timeout_sec = 500
    }
  }
}

