
---

# Sales Analysis – Data Engineering Pipeline

This project implements an end-to-end data engineering pipeline for sales analysis using modern tools like Google Cloud Storage, BigQuery, Dataproc, Terraform, DLT, dbt, Airflow, and Power BI. The first step involves provisioning the Google Cloud infrastructure using Terraform.

## Overview

The pipeline’s infrastructure comprises:

- **Google Cloud Storage Bucket:** For storing raw and processed sales data, with lifecycle rules to manage retention and aborted uploads.
- **BigQuery Datasets:** Two separate datasets:
  - One for raw/cleaned data (`sales_cleaned_raw`)
  - One for transformed data (`sales_marts`)
- **Dataproc Cluster:** A single-node cluster (with auto-deletion) configured for data cleaning and preprocessing.
- **IAM Role Assignments:** A dedicated service account, whose email is dynamically extracted from the provided credentials file, is used for deploying and managing resources. The service account is granted minimal—but sufficient—permissions to perform the following operations:
  - **Dataproc Management:** `roles/dataproc.editor`
  - **Storage Management:** `roles/storage.admin` and `roles/storage.objectAdmin`  
    *(The `roles/storage.objectAdmin` role is essential for allowing the service account to read and update objects in custom staging and temporary buckets used by Dataproc.)*
  - **BigQuery Management:** `roles/bigquery.dataEditor`
  - **Compute Management:** `roles/compute.instanceAdmin`
  - **IAM Policy Management:** `roles/iam.securityAdmin` and `roles/iam.serviceAccountUser`

## Step 1: Provision Infrastructure with Terraform

Terraform is used to set up the following key components on Google Cloud:

1. **Provider and Service Account Email Extraction:**
   - The configuration reads your service account credentials from a JSON file.
   - It dynamically extracts the `client_email` (used as the service account) so you do not have to hardcode this value.

2. **Resource Provisioning:**
   - **Cloud Storage Bucket:** A bucket is created for staging sales data along with lifecycle rules (delete objects older than 7 days and abort incomplete multipart uploads after 1 day).
   - **BigQuery Datasets:** Two datasets are provisioned for storing raw (cleaned) and transformed data.
   - **Dataproc Cluster:** A single-node cluster is created to perform data cleaning. The configuration specifies custom staging and temporary buckets so that the service account (with the appropriate storage roles) can access them. An auto-deletion TTL is configured to avoid unnecessary costs.
  
3. **IAM Role Assignments:**
   - A dedicated module assigns all the necessary IAM roles to the service account. In addition to roles for managing Dataproc, BigQuery, Compute, and Storage, the service account is granted the `roles/storage.objectAdmin` role. This role is required to grant the service account permissions on custom staging and temporary buckets that Dataproc uses for task execution and file storage.

## Getting Started

1. **Configure Your Environment:**
   - Place your credentials file (e.g., `keys/my-creds.json`) in the appropriate directory.
   - Edit the `terraform/variables.tf` file if needed to adjust defaults, such as project ID, region, or resource names.

2. **Initialize and Apply Terraform:**
   - Navigate to the Terraform directory and run:
     ```bash
     terraform -chdir=terraform/ init
     terraform -chdir=terraform/ plan
     terraform -chdir=terraform/ apply
     ```
   - Terraform will then deploy your GCP infrastructure (storage, datasets, cluster, and IAM role assignments) accordingly.
 
---

## Step 2: Download and Upload Sales Data to GCS

After you have provisioned your infrastructure with Terraform, the next step is to ingest your raw sales data into your Google Cloud Storage bucket. For this purpose, you will run a Bash script that automates the following actions:

1. **Download the Dataset:** The script fetches the latest sample sales dataset from Kaggle using a direct URL with `curl`.
2. **Unzip and Extract:** It then unzips the downloaded archive and extracts the CSV file that contains the raw sales data.
3. **Upload to GCS:** Finally, the script uploads the extracted CSV file to your designated GCS bucket (e.g., `sales_data_bucket`) using `gsutil`.
4. **Upload Preprocessing Script:** It also uploads the PySpark preprocessing script (`Sales_Data_Cleaning_And_Aggregation.py`) to the `code/` folder in the same bucket, ensuring Dataproc can access and execute it in the next step.

To execute this step, navigate to the project’s root directory and run the Bash script (located in the `scripts/` folder) after making it executable. This completes the data ingestion phase and prepares your raw dataset for further processing in later steps.

```bash
.\scripts\download_and_upload.sh
```
## Step 3: Run the Cleaning & Aggregation Job on Dataproc


Now that your raw data and preprocessing script are uploaded, trigger the PySpark job on your Dataproc cluster. This command fetches the script from GCS, supplies all necessary parameters, and runs the data cleaning and aggregation workflow:

```bash

gcloud dataproc jobs submit pyspark \
    --cluster=sales-cleaning-cluster \
    --region=us-central1 \
    gs://sales_data_bucket_project_cider/code/Sales_Data_Cleaning_And_Aggregation.py \
    -- \
        --input_path=gs://sales_data_bucket_project_cider/sales_06_FY2020-21 copy.csv \
		--output_raw_bq=sales_cleaned_raw.sales_raw_tab \
		--output_parquet=gs://sales_data_bucket_project_cider/processed/cleaned_sales_data \
		--output_bq=sales_marts.sales_processed_cleaned_tab \
		--output_agg_parquet=gs://sales_data_bucket_project_cider/aggregated/cleaned_aggregated_sales_data \
		--output_agg_bq=sales_marts.cleaned_aggregated_sales_tab
   ```
## Step 4:
