#!/bin/bash
# download_and_upload.sh
# This script downloads the sample sales data zip file from Kaggle using curl,
# extracts the CSV file, and uploads it to a specified GCS bucket.

# Set variables
BUCKET_NAME="sales_data_bucket_project_cider"
DATA_DIR="./raw_data"
DOWNLOAD_DEST="$HOME/Downloads/sample-sales-data.zip"
KAGGLE_DOWNLOAD_URL="https://www.kaggle.com/api/v1/datasets/download/kyanyoga/sample-sales-data"

# Create data directory if it doesn't exist
mkdir -p "$DATA_DIR"

# Download the dataset using curl
echo "Downloading sample sales data from Kaggle..."
curl -L -o "$DOWNLOAD_DEST" "$KAGGLE_DOWNLOAD_URL"

if [ $? -ne 0 ]; then
  echo "Error downloading dataset. Exiting."
  exit 1
fi

# Move the downloaded zip into our DATA_DIR
mv "$DOWNLOAD_DEST" "$DATA_DIR/"

# Unzip the downloaded dataset
ZIP_FILE=$(find "$DATA_DIR" -maxdepth 1 -type f -name "*.zip" | head -n 1)
if [ -z "$ZIP_FILE" ]; then
  echo "Error: No ZIP file found in $DATA_DIR"
  exit 1
fi

echo "Unzipping dataset: $ZIP_FILE..."
unzip -o "$ZIP_FILE" -d "$DATA_DIR"

# Find the CSV file (assuming there's only one CSV file)
CSV_FILE=$(find "$DATA_DIR" -type f -name "*.csv" | head -n 1)
if [ -z "$CSV_FILE" ]; then
  echo "Error: No CSV file found after unzipping."
  exit 1
fi

echo "Found CSV file: $CSV_FILE"

# Upload the CSV file to the GCS bucket
echo "Uploading $CSV_FILE to gs://$BUCKET_NAME/ ..."
gsutil cp "$CSV_FILE" "gs://$BUCKET_NAME/"

if [ $? -eq 0 ]; then
  echo "Upload successful!"
else
  echo "Upload failed."
  exit 1
fi

# Upload the PySpark script to the GCS bucket
SCRIPT_PATH="./scripts//Sales_Data_Cleaning_And_Aggregation.py"
if [ -f "$SCRIPT_PATH" ]; then
  echo "Uploading $SCRIPT_PATH to gs://$BUCKET_NAME/code/ ..."
  gsutil cp "$SCRIPT_PATH" "gs://$BUCKET_NAME/code/"
  if [ $? -eq 0 ]; then
    echo "Script upload successful!"
  else
    echo "Script upload failed."
    exit 1
  fi
else
  echo "Error: $SCRIPT_PATH not found."
  exit 1
fi

# Upload the PySpark script to the GCS bucket
SCRIPT_PATH="./scripts//initialization_action.sh"
if [ -f "$SCRIPT_PATH" ]; then
  echo "Uploading $SCRIPT_PATH to gs://$BUCKET_NAME/code/ ..."
  gsutil cp "$SCRIPT_PATH" "gs://$BUCKET_NAME/code/"
  if [ $? -eq 0 ]; then
    echo "Script upload successful!"
  else
    echo "Script upload failed."
    exit 1
  fi
else
  echo "Error: $SCRIPT_PATH not found."
  exit 1
fi