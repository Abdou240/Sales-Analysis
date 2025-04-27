#!/usr/bin/env python
# coding: utf-8
import dlt
from dlt.destinations.adapters import bigquery_adapter       
import gcsfs   
import argparse
import pyspark
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    DateType,
    StructType,
    StructField,
    BooleanType
)
# import os
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/etc/gcp/creds.json"


# **Verify the ADC env var and file existence**
# creds_path = "/etc/gcp/creds.json"
# print("GOOGLE_APPLICATION_CREDENTIALS=", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
# print(f"File exists at {creds_path}:", os.path.exists(creds_path))
# if os.path.exists(creds_path):
#     print("File size:", os.path.getsize(creds_path), "bytes")

import dlt
from dlt.sources import incremental
from pyspark.sql import DataFrame
import logging
logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    level=logging.INFO
)
logging.getLogger("dlt").setLevel(logging.INFO)

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
    .appName("SalesDataCleaningAndDLT") \
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
# rename "Phone No. "to "phone"
df = df.withColumnRenamed("Phone No. ", "phone")

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
# ---------------------------------------------------------------------
df = df.dropDuplicates().dropDuplicates(["order_id", "item_id"])

# ---------------------------------------------------------------------
#  4. Drop rows with nulls or empty strings
# ---------------------------------------------------------------------
required = ["order_id", "order_date", "item_id", "qty_ordered", "price"]
df = df.na.drop(subset=required)
non_empty = reduce(lambda acc, c: acc & (F.trim(F.col(c)) != ""), required, F.lit(True))
df = df.filter(non_empty)
# ---------------------------------------------------------------------
#  5. Cast, rename, parse dates, and select relevant columns
# ---------------------------------------------------------------------
# 1) Cast all raw columns to their correct types
df_typed = (
    df
    .withColumn("order_id",           F.col("order_id").cast(LongType()))
    .withColumn("order_date",         F.to_date("order_date", "M/d/yyyy").cast(DateType()))
    .withColumn("status",             F.col("status").cast(StringType()))
    .withColumn("item_id",            F.col("item_id").cast(LongType()))
    .withColumn("sku",                F.col("sku").cast(StringType()))
    .withColumn("qty_ordered",        F.col("qty_ordered").cast(DoubleType()))
    .withColumn("price",              F.col("price").cast(DoubleType()))
    .withColumn("value",              F.col("value").cast(DoubleType()))
    .withColumn("discount_amount",    F.col("discount_amount").cast(DoubleType()))
    .withColumn("total",              F.col("total").cast(DoubleType()))
    .withColumn("category",           F.col("category").cast(StringType()))
    .withColumn("payment_method",     F.col("payment_method").cast(StringType()))
    .withColumn("bi_st",              F.col("bi_st").cast(StringType()))
    .withColumn("cust_id",            F.col("cust_id").cast(LongType()))
    .withColumn("year",               F.col("year").cast(LongType()))
    .withColumn("month",              F.col("month").cast(StringType()))
    .withColumn("ref_num",            F.col("ref_num").cast(LongType()))
    .withColumn("Name Prefix",        F.col("Name Prefix").cast(StringType()))
    .withColumn("First Name",         F.col("First Name").cast(StringType()))
    .withColumn("Middle Initial",     F.col("Middle Initial").cast(StringType()))
    .withColumn("Last Name",          F.col("Last Name").cast(StringType()))
    .withColumn("Gender",             F.col("Gender").cast(StringType()))
    .withColumn("age",                F.col("age").cast(DoubleType()))
    .withColumn("full_name",          F.col("full_name").cast(StringType()))
    .withColumn("E Mail",             F.col("E Mail").cast(StringType()))
    .withColumn("Customer Since",     F.to_date("Customer Since", "M/d/yyyy").cast(DateType()))
    .withColumn("SSN",                F.col("SSN").cast(StringType()))
    .withColumn("phone",              F.col("phone").cast(StringType()))
    .withColumn("Place Name",         F.col("Place Name").cast(StringType()))
    .withColumn("County",             F.col("County").cast(StringType()))
    .withColumn("City",               F.col("City").cast(StringType()))
    .withColumn("State",              F.col("State").cast(StringType()))
    .withColumn("Zip",                F.col("Zip").cast(StringType()))
    .withColumn("Region",             F.col("Region").cast(StringType()))
    .withColumn("User Name",          F.col("User Name").cast(StringType()))
    .withColumn("Discount_Percent",   F.col("Discount_Percent").cast(DoubleType()))
)



df = df_typed \
    .withColumnRenamed("qty_ordered",      "quantity") \
    .withColumnRenamed("price",            "unit_price") \
    .withColumnRenamed("value",            "line_value") \
    .withColumnRenamed("total",            "total_amount") \
    .withColumnRenamed("Customer Since",   "customer_since") \
    .withColumnRenamed("cust_id",          "customer_id") \
    .withColumnRenamed("sku", "sku") \
    .withColumnRenamed("category", "category") \
    .withColumnRenamed("payment_method", "payment_method") \
    .withColumnRenamed("ref_num", "ref_num") \
    .withColumnRenamed("First Name", "first_name") \
    .withColumnRenamed("Last Name", "last_name") \
    .withColumnRenamed("full_name", "full_name") \
    .withColumnRenamed("Gender", "gender") \
    .withColumnRenamed("E Mail", "email") \
    .withColumnRenamed("City", "city") \
    .withColumnRenamed("State", "state") \
    .withColumnRenamed("Region", "region") \
    .withColumnRenamed("Name Prefix", "name_prefix") \
    .withColumnRenamed("Middle Initial", "middle_initial") \
    .withColumnRenamed("User Name", "user_name") \
    .withColumnRenamed("SSN", "social_security_number") \
    .withColumnRenamed("Zip", "zip")

# 1) Count nulls before dropping
null_count = df.filter(F.col("order_id").isNull()).count()
print(f"Number of rows with NULL order_id: {null_count}")

# 2) Drop rows where order_id IS NULL
df = df.dropna(subset=["order_id"])

# 3) Count rows after dropping
remaining_count = df.count()
print(f"Number of rows after dropping NULL order_id: {remaining_count}")


# ---------------------------------------------------------------------
#  6. Add a new Column "free_products" and Validate total matches value minus discount
# ---------------------------------------------------------------------
# add new Column "free_products"
df = df.withColumn(
    "free_products",
    F.col("quantity") - (F.col("line_value") / F.col("unit_price"))
)
# Reorder the columns to place 'gifted_products' after 'qty_ordered'
columns = df.columns
qty_ordered_index = columns.index('quantity')
new_columns_order = columns[:qty_ordered_index + 1] + ['free_products'] + columns[qty_ordered_index + 1:-1]

df = df.select(*new_columns_order)
# Validate total matches value minus discount
df = df.withColumn(
    "computed_total",
    F.col("line_value") - F.col("discount_amount")
)
df = df.filter(
    F.abs(F.col("total_amount") - F.col("computed_total")) < 1e-6
).drop("computed_total")


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
  category,
  payment_method,
  date_trunc('month', order_date) AS month,
  COUNT(DISTINCT order_id) AS orders_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(quantity), 2) AS avg_quantity,
  ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM cleaned_sales
GROUP BY 1, 2, 3
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


# ---------------------------------------------------------------------
# 11. dim_customers (SCD2)
# ---------------------------------------------------------------------
@dlt.resource(
    name="dim_customers",
    primary_key="customer_id",  
    merge_key="customer_since",
    write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def dim_customers():
    # 1) Add a month‐partition column
    df_with_month = df.withColumn(
        "customer_since_month",
        F.date_trunc("month", F.col("customer_since")).cast(DateType())
    )   
    sel = (
        df_with_month.select(
            "customer_id",
            "ref_num",
            "name_prefix",
            "full_name",
            "first_name",
            "middle_initial",
            "last_name",
            "gender",
            "age",
            "email",
            "customer_since",
            "customer_since_month",
            "social_security_number",
            "phone",
            "user_name",
            "zip"
        )
        .dropDuplicates(["customer_id"])
    )
    # yield dicts so DLT can infer schema & do a merge
    for row in sel.collect():
        yield row.asDict()

# ---------------------------------------------------------------------
# 12. dim_products (SCD2)
# ---------------------------------------------------------------------
@dlt.resource(
    name="dim_products",
    primary_key="item_id",
    merge_key="category",
    write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def dim_products():
    sel = (
        df
        .select("item_id", "sku", "category")
        .dropDuplicates(["item_id"])
    )
    for row in sel.collect():
        yield row.asDict()
# ---------------------------------------------------------------------
# 13. dim_location
# ---------------------------------------------------------------------
@dlt.resource(
    name="dim_location",
    primary_key="zip",
    write_disposition={"disposition": "merge"}
)
def dim_location():
    sel = (
        df
        .select(
            F.col("zip"),
            F.col("Place Name").alias("place_name"),
            F.col("County").alias("county"),
            F.col("city"),
            F.col("state"),
            F.col("region")
        )
        .dropDuplicates(["zip"])
    )
    for row in sel.collect():
        yield row.asDict()
# ---------------------------------------------------------------------
# 14. dim_date 
# ---------------------------------------------------------------------
@dlt.resource(
    name="dim_date",
    primary_key="date_key",
    merge_key="month",
    write_disposition={"disposition": "merge"}
)
def dim_date():
    df_dates = (
        df
        .select(F.col("order_date").alias("date"))
        .dropDuplicates(["date"])
        .withColumn("date_key",   F.date_format("date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year",       F.year("date"))
        .withColumn("quarter",    F.concat(F.lit("Q"), F.quarter("date")))
        .withColumn("month",      F.date_format("date", "MMMM"))
        .withColumn("day_of_week",F.date_format("date", "EEEE"))
        # use dayofweek() to flag weekends
        .withColumn(
            "is_weekend",
            F.dayofweek("date").isin(1, 7).cast(BooleanType())
        )
    )

    for row in df_dates.collect():
        yield row.asDict()

# ---------------------------------------------------------------------
# 15. fact_orders(append‐only)
# ---------------------------------------------------------------------
@dlt.resource(
    name="fact_orders",
    primary_key= "order_id",
    write_disposition={"disposition": "append"}
)
def fact_orders():
    sel = df.select(
        "order_id", "order_date", "status",
        "item_id",
        "quantity", "free_products", "unit_price",
        "line_value", "discount_amount", "total_amount",
        "discount_percent",
        "payment_method", "bi_st",
        "year", "month",
        "customer_id"
    )
    for row in sel.collect():
        yield row.asDict()

# ---------------------------------------------------------------------
# 16. Build DLT pipeline
# ---------------------------------------------------------------------

pipeline = dlt.pipeline(
    pipeline_name="sales_dlt_to_bq",
    destination="bigquery",
    dataset_name="normalized_sales",
    full_refresh=True 
)

# ---------------------------------------------------------------------
# 16. Run pipeline
# ---------------------------------------------------------------------

pipeline.run(bigquery_adapter(
    dim_customers,
    partition="customer_since_month",
    cluster=["customer_id"]
))

pipeline.run(bigquery_adapter(
    dim_products,
    partition="category"
))

pipeline.run(bigquery_adapter(
    dim_location,
    partition="region"
))

pipeline.run(bigquery_adapter(
    dim_date,
    partition="month"
))

pipeline.run(bigquery_adapter(
    fact_orders,
    partition="month"
))
