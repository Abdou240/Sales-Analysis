#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------
#  Argument parsing
# ---------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_path",
    required=True,
    help="GCS path to the raw CSV file, e.g. gs://.../raw_data/sample-sales-data.csv"
)
parser.add_argument(
    "--output_parquet",
    required=True,
    help="GCS path to write cleaned Parquet data, e.g. gs://.../processed/cleaned_sales_data"
)
parser.add_argument(
    "--output_bq",
    required=True,
    help="BigQuery table to write cleaned data, e.g. project.dataset.cleaned_sales"
)
parser.add_argument(
    "--output_agg_parquet",
    required=True,
    help="GCS path to write aggregated Parquet data, e.g. gs://.../processed/agg_sales_data"
)
parser.add_argument(
    "--output_agg_bq",
    required=True,
    help="BigQuery table to write aggregated data, e.g. project.dataset.agg_sales"
)
parser.add_argument(
    "--output_raw_bq",
    required=True,
    help="BigQuery table to store raw data before preprocessing, e.g. project.dataset.raw_sales"
)

args = parser.parse_args()

input_path         = args.input_path
output_parquet     = args.output_parquet
output_bq          = args.output_bq
output_agg_parquet = args.output_agg_parquet
output_agg_bq      = args.output_agg_bq
output_raw_bq       = args.output_raw_bq

# ---------------------------------------------------------------------
#  Spark session
# ---------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SalesDataCleaningAndAggregation") \
    .getOrCreate()

# Set GCS temporary bucket for BigQuery writes
spark.conf.set('temporaryGcsBucket', 'custom-dataproc-temp-bucket')

print(f"Spark Version: {spark.version}")

# Accessing the Scala version (indirectly through the sSpark context)
sc = spark.sparkContext
scala_version = sc._jvm.scala.util.Properties.versionString()
print(f"Scala Version: {scala_version}")


# ---------------------------------------------------------------------
#  1. Load raw CSV
# ---------------------------------------------------------------------
df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

# ---------------------------------------------------------------------
#  2. Store raw data to BigQuery
#     • Overwrite existing raw table if present
# ---------------------------------------------------------------------
df.write \
  .format("bigquery") \
  .mode("overwrite") \
  .option("provider", "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider") \
  .option("table", output_raw_bq) \
  .save()

# ---------------------------------------------------------------------
#  3. Filter & deduplicate
#     • Only shipped orders
#     • Drop exact duplicates
#     • Drop duplicates on (ORDERNUMBER, ORDERLINENUMBER)
# ---------------------------------------------------------------------
df = df.filter(F.col("STATUS") == "Shipped") \
       .dropDuplicates() \
       .dropDuplicates(["ORDERNUMBER", "ORDERLINENUMBER"])

# ---------------------------------------------------------------------
#  4. Drop rows with nulls or empty strings in critical columns
# ---------------------------------------------------------------------
required_cols = ["ORDERNUMBER", "ORDERDATE", "PRODUCTCODE", "QUANTITYORDERED", "PRICEEACH"]
# drop nulls
df = df.na.drop(subset=required_cols)
# drop empty strings
non_empty = reduce(
    lambda acc, c: acc & (F.trim(F.col(c)) != ""),
    required_cols,
    F.lit(True)
)
df = df.filter(non_empty)

# ---------------------------------------------------------------------
#  5. Cast, rename, parse dates, preserve original order
# ---------------------------------------------------------------------
df = df.select(
    F.col("ORDERNUMBER"       ).cast("integer").alias("order_number"),
    F.col("ORDERLINENUMBER"   ).cast("integer").alias("order_line_no"),
    F.col("QUANTITYORDERED"   ).cast("integer").alias("quantity"),
    F.col("PRICEEACH"         ).cast("double" ).alias("unit_price"),
    F.col("SALES"             ).cast("double" ).alias("sales_amount"),
    F.to_timestamp("ORDERDATE", "M/d/yyyy H:mm").alias("order_date"),
    F.col("STATUS"            ).alias("status"),
    F.col("QTR_ID"            ).cast("integer").alias("quarter_id"),
    F.col("MONTH_ID"          ).cast("integer").alias("month_id"),
    F.col("YEAR_ID"           ).cast("integer").alias("year_id"),
    F.col("PRODUCTLINE"       ).alias("product_line"),
    F.col("MSRP"              ).cast("double" ).alias("msrp"),
    F.col("PRODUCTCODE"       ).alias("product_code"),
    F.col("CUSTOMERNAME"      ).alias("customer_name"),
    F.col("PHONE"             ).alias("phone"),
    F.col("ADDRESSLINE1"      ).alias("address_line1"),
    F.col("ADDRESSLINE2"      ).alias("address_line2"),
    F.col("CITY"              ).alias("city"),
    F.col("STATE"             ).alias("state"),
    F.col("POSTALCODE"        ).alias("postal_code"),
    F.col("COUNTRY"           ).alias("country"),
    F.col("TERRITORY"         ).alias("territory"),
    F.col("CONTACTLASTNAME"   ).alias("contact_last_name"),
    F.col("CONTACTFIRSTNAME"  ).alias("contact_first_name"),
    F.col("DEALSIZE"          ).alias("deal_size")
)

# ---------------------------------------------------------------------
#  6. Validate numeric fields & sales consistency
#     • quantity > 0, unit_price > 0
#     • sales_amount within 1% of quantity * unit_price
# ---------------------------------------------------------------------
df = df.filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
df = df.withColumn("expected_sales", F.col("quantity") * F.col("unit_price"))
df = df.filter(
    (F.abs(F.col("sales_amount") - F.col("expected_sales")) / F.col("expected_sales")) < 0.01
).drop("expected_sales")

# ---------------------------------------------------------------------
#  7. Normalize string columns
#     • Fill nulls with empty string, trim whitespace
#     • Standardize state codes to uppercase
# ---------------------------------------------------------------------
string_cols = [c for c, t in df.dtypes if t == "string"]
for col in string_cols:
    df = df.withColumn(
        col,
        F.trim(F.when(F.col(col).isNull(), F.lit("")).otherwise(F.col(col)))
    )
df = df.withColumn("state", F.upper(F.col("state")))

# ---------------------------------------------------------------------
#  8. Write cleaned data to Parquet and BigQuery
# ---------------------------------------------------------------------
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .parquet(output_parquet)

									 
																	   
df.write \
  .format("bigquery") \
  .mode("overwrite") \
  .option("provider", "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider") \
  .option("table", output_bq) \
  .mode("overwrite") \
  .save()

# ---------------------------------------------------------------------
#  9. Register cleaned data as temp table and aggregate
# ---------------------------------------------------------------------
df.createOrReplaceTempView("cleaned_sales")

# ROUND(..., 2) Used for financial precision
agg_query = """
SELECT
  product_line,
  date_trunc('month', order_date) AS month,
  COUNT(DISTINCT order_number) AS orders_count,
  ROUND(SUM(sales_amount), 2) AS total_revenue,
  ROUND(AVG(quantity), 2) AS avg_quantity,
  ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM cleaned_sales
GROUP BY 1, 2
"""
df_result = spark.sql(agg_query)

# ---------------------------------------------------------------------
# 10. Write aggregated results to Parquet and BigQuery
# ---------------------------------------------------------------------
df_result.coalesce(1) \
  .write \
  .mode("overwrite") \
  .parquet(output_agg_parquet)

df_result.write \
  .format("bigquery") \
  .mode("overwrite") \
  .option("provider", "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider") \
  .option("table", output_agg_bq) \
  .mode("overwrite") \
  .save()


