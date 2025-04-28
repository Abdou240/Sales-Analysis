variable "credentials_path" {
  description = "Path to the service account credentials JSON file"
  type        = string
  default     = "keys/my-creds.json"
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "vigilant-cider-376613"
}

variable "region" {
  description = "The default region for resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
  default     = "sales_data_bucket_project_cider"
}

variable "bucket_location" {
  description = "The location of the GCS bucket"
  type        = string
  default     = "US"
}

variable "raw_dataset_id" {
  description = "Dataset ID for raw data"
  type        = string
  default     = "sales_cleaned_raw"
}

variable "marts_dataset_id" {
  description = "Dataset ID for transformed data"
  type        = string
  default     = "sales_marts"
}

variable "dataset_location" {
  description = "Location of the BigQuery datasets"
  type        = string
  default     = "US"
}

variable "cluster_name" {
  description = "Name of the Dataproc cluster"
  type        = string
  default     = "sales-cleaning-cluster"
}

variable "cluster_region" {
  description = "Region of the Dataproc cluster"
  type        = string
  default     = "us-central1"
}

variable "cluster_zone" {
  description = "Zone of the Dataproc cluster"
  type        = string
  default     = "us-central1-a"
}

variable "cluster_machine_type" {
  description = "Machine type for the Dataproc master node"
  type        = string
  default     = "n4-standard-2"
}

variable "boot_disk_type" {
  description = "Boot disk type for Dataproc master node"
  type        = string
  default     = "hyperdisk-balanced"
}

variable "auto_delete_ttl" {
  description = "TTL for Dataproc cluster auto-deletion"
  type        = string
  default     = "3600s"
}

variable "dataproc_staging_bucket" {
  description = "Name of the custom Dataproc staging bucket"
  type        = string
  default     = "custom-dataproc-staging-bucket"
}

variable "dataproc_temp_bucket" {
  description = "Name of the custom Dataproc temporary bucket"
  type        = string
  default     = "custom-dataproc-temp-bucket"
}

variable "gcs_object_name" {
  description = "Name/path of the initialization script in the GCS bucket"
  type        = string
  default     = "scripts/initialization_action.sh"  # Default path in bucket
}

variable "local_script_path" {
  description = "Local path to the initialization script file"
  type        = string
  default     = "../scripts/initialization_action.sh"  # Default local path
}