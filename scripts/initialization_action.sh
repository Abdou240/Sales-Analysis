#!/bin/bash
set -e

# 1) Install the DLT Python package
pip install "dlt[bigquery,parquet]" gcsfs 