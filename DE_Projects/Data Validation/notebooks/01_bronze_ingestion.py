# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC This notebook ingests raw CSV files from Unity Catalog Volumes into the Bronze layer Delta tables.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Incremental file ingestion (processes only new files)
# MAGIC - Enforces predefined schema for each entity
# MAGIC - Adds audit columns for traceability
# MAGIC - Performs basic QC checks: row count, schema mismatch, file completeness
# MAGIC
# MAGIC **Source:** Unity Catalog Volume  
# MAGIC **Destination:** `qc_validation.bronze.*` Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration Setup

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/negigeetanshusingh@gmail.com/Projects/Data Validation/config')
sys.path.append('/Workspace/Users/negigeetanshusingh@gmail.com/Projects/Data Validation/utils')

from project_config import *


# COMMAND ----------

# ----------------------------------------
# Import necessary configuration and utility modules
# ----------------------------------------
import sys


# Import project-level constants and config functions
from project_config import (
    CATALOG_NAME, BRONZE_SCHEMA,
    BRONZE_CUSTOMERS_TABLE, BRONZE_PRODUCTS_TABLE, 
    BRONZE_STORES_TABLE, BRONZE_TRANSACTIONS_TABLE,
    VOLUME_PATH, BRONZE_SOURCE_PATH,
    CUSTOMERS_FILE_PATTERN, PRODUCTS_FILE_PATTERN,
    STORES_FILE_PATTERN, TRANSACTIONS_FILE_PATTERN,
    PROCESSED_FILES_TABLE,
    get_batch_id, get_ingestion_date,
    BRONZE_CUSTOMERS_SCHEMA, BRONZE_PRODUCTS_SCHEMA,
    BRONZE_STORES_SCHEMA, BRONZE_TRANSACTIONS_SCHEMA
)

# Import helper functions for ingestion logic
from helpers import (
    get_new_files, mark_file_as_processed,
    add_audit_columns, create_table_if_not_exists
)

# Import Quality Check (QC) framework utilities
from qc_framework import (
    check_row_count, check_schema, run_qc_checks, save_qc_results
)

# Import Spark modules and standard libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from datetime import datetime
import time

# ----------------------------------------
# Initialize Spark session
# ----------------------------------------
spark = SparkSession.builder.appName("Bronze_Ingestion").getOrCreate()

# Generate unique batch ID and ingestion timestamp for tracking
batch_id = get_batch_id()
ingestion_date = get_ingestion_date()

print(f"Starting Bronze Ingestion - Batch ID: {batch_id}")
print(f"Ingestion Date: {ingestion_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Bronze Tables

# COMMAND ----------

# ----------------------------------------
# Ensure required catalog and schema exist
# Also create a table to track processed files
# ----------------------------------------

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# Define processed files tracking table
processed_files_table = PROCESSED_FILES_TABLE 

# Create tracking table to record processed file paths, batch IDs, and timestamps
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {processed_files_table} (
        file_path STRING,
        batch_id STRING,
        row_count INT,
        processed_at TIMESTAMP
    )
    USING DELTA
""")

print(f"Created processed files tracking table: {processed_files_table}")
print(f"Volume path: {BRONZE_SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Define Schema Functions

# COMMAND ----------

# ----------------------------------------
# Define schemas for each data entity
# ----------------------------------------

# Schema for customers data
def get_customers_schema():
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_date", TimestampType(), True),
        StructField("_updated_at", TimestampType(), True)])

# Schema for products data
def get_products_schema():
    return StructType([
        StructField("sku", StringType(), True),
        StructField("product_key", StringType(), True),
        StructField("category", StringType(), True),
        StructField("list_price", DoubleType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_date", TimestampType(), True),
        StructField("_updated_at", TimestampType(), True)])

# Schema for stores data
def get_stores_schema():
    return StructType([
        StructField("store_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_date", TimestampType(), True),
        StructField("_updated_at", TimestampType(), True)
    ])

# Schema for transactions data
def get_transactions_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("transaction_line_id", StringType(), True),
        StructField("transaction_ts", TimestampType(), True),
        StructField("channel", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_date", TimestampType(), True),
        StructField("_updated_at", TimestampType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Ingestion Function

# COMMAND ----------


def ingest_csv_to_bronze(entity_name: str, 
                         file_pattern: str, 
                         schema: StructType,
                         target_table: str,
                         header: bool = True):
    """
    Ingest CSV files from Unity Catalog volume to Bronze layer Delta tables
    """
    
    print(f"\n{'='*60}")
    print(f"Processing {entity_name}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Step 1: List available CSV files in source volume
    try:
        all_files = []
        print(f"Listing files in volume: {BRONZE_SOURCE_PATH}")
        pattern_keyword = file_pattern.replace("*.csv", "").replace("*", "").lower()
        
        # Use Databricks dbutils to list files in the volume
        files = dbutils.fs.ls(BRONZE_SOURCE_PATH)
        for file_info in files:
            file_name = file_info.name
            file_path = file_info.path
            
            # Match file pattern and ensure it's a CSV
            if not file_info.isDir() and pattern_keyword in file_name.lower() and file_name.endswith(".csv"):
                all_files.append(file_path)
                print(f"  Found file: {file_name}")
        
        # Step 2: Fetch already processed files to avoid duplicates
        processed_files = []
        try:
            processed_df = spark.table(processed_files_table)
            processed_files = [row.file_path for row in processed_df.select("file_path").distinct().collect()]
            print(f"Found {len(processed_files)} previously processed files")
        except Exception as e:
            print("No processed files found yet (first run).")
        
        # Identify new files that haven’t been processed yet
        new_files = [f for f in all_files if f not in processed_files]
        
    except Exception as e:
        print(f"Error listing files using dbutils: {str(e)}")
        # Fallback to helper function
        new_files = get_new_files(spark, BRONZE_SOURCE_PATH, file_pattern, processed_files_table)
    
    # Exit if no new files are found
    if not new_files:
        print(f"No new files found for {entity_name}. Skipping ingestion.")
        return
    
    print(f"Found {len(new_files)} new file(s) for {entity_name}")
    for file in new_files:
        print(f"  - {file}")
    
    # Step 3: Read and process each new file
    all_dataframes = []
    
    for file_path in new_files:
        print(f"\nProcessing file: {file_path}")
        
        try:
            # Read CSV with predefined schema
            df = spark.read \
                .option("header", header) \
                .option("inferSchema", "false") \
                .schema(schema) \
                .csv(file_path)
            
            # Add audit columns for traceability
            file_name = file_path.split("/")[-1]
            df = add_audit_columns(df, batch_id, file_name)
            
            # Perform basic QC: row count
            row_count = df.count()
            print(f"  Rows read: {row_count}")
            
            if row_count == 0:
                print(f"  WARNING: File {file_name} has no rows.")
            
            # Validate schema
            schema_result = check_schema(df, target_table, schema)
            if schema_result.status == "FAIL":
                raise Exception(f"Schema validation failed: {schema_result.message}")
            
            # Add DataFrame to list for later union
            all_dataframes.append(df)
            
            # Record file as processed
            print('Before mark_file_as_processed')
            mark_file_as_processed(spark, processed_files_table, file_path, batch_id, row_count)
            
        except Exception as e:
            print(f"  ERROR processing file {file_path}: {str(e)}")
            raise
    
    # Step 4: Combine all processed DataFrames and write to Bronze table
    if all_dataframes:
        combined_df = all_dataframes[0]
        for df in all_dataframes[1:]:
            combined_df = combined_df.union(df)
        
        total_rows = combined_df.count()
        print(f"\nTotal rows to ingest for {entity_name}: {total_rows}")
        
        # Append new data into Bronze Delta table
        combined_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table)
        
        print(f"✅ Successfully ingested {total_rows} rows to {target_table}")
        
        # Step 5: Run Quality Control checks
        print(f"\nRunning QC checks...")
        qc_checks = {"row_count": {"min_rows": 1}}
        qc_results = run_qc_checks(combined_df, target_table, qc_checks)
        
        for result in qc_results:
            print(f"  {result.check_name}: {result.status} - {result.message}")
        
        print(f"⏱️ Processing completed in {time.time() - start_time:.2f} seconds")
    
    else:
        print(f"No data ingested for {entity_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Ingest Customers

# COMMAND ----------

# Ingest customers CSV files into Bronze layer
ingest_csv_to_bronze(
    entity_name="customers",
    file_pattern=CUSTOMERS_FILE_PATTERN,
    schema=get_customers_schema(),
    target_table=BRONZE_CUSTOMERS_TABLE,
    header=True
)

#/Volumes/qc_validation/raw_files/data/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Ingest Products

# COMMAND ----------

# Ingest products CSV files into Bronze layer
ingest_csv_to_bronze(
    entity_name="products",
    file_pattern=PRODUCTS_FILE_PATTERN,
    schema=get_products_schema(),
    target_table=BRONZE_PRODUCTS_TABLE,
    header=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Ingest Stores

# COMMAND ----------

# Ingest stores CSV files into Bronze layer
ingest_csv_to_bronze(
    entity_name="stores",
    file_pattern=STORES_FILE_PATTERN,
    schema=get_stores_schema(),
    target_table=BRONZE_STORES_TABLE,
    header=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Ingest Transactions

# COMMAND ----------

# Ingest transactions CSV files into Bronze layer
ingest_csv_to_bronze(
    entity_name="transactions",
    file_pattern=TRANSACTIONS_FILE_PATTERN,
    schema=get_transactions_schema(),
    target_table=BRONZE_TRANSACTIONS_TABLE,
    header=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Verify Ingestion

# COMMAND ----------

# ----------------------------------------
# Validate data load by checking row counts
# for all Bronze tables
# ----------------------------------------

print("Bronze Layer Row Counts:")
print("=" * 60)

tables = [
    ("Customers", BRONZE_CUSTOMERS_TABLE),
    ("Products", BRONZE_PRODUCTS_TABLE),
    ("Stores", BRONZE_STORES_TABLE),
    ("Transactions", BRONZE_TRANSACTIONS_TABLE)
]

for table_name, table_path in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_path}").collect()[0]["cnt"]
        print(f"{table_name:20s}: {count:>10,} rows")
    except Exception as e:
        print(f"{table_name:20s}: ERROR - {str(e)}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Summary

# COMMAND ----------

# ----------------------------------------
# Final summary of ingestion run
# ----------------------------------------
print(f"""
Bronze Ingestion Completed
{'='*60}
Batch ID: {batch_id}
Ingestion Date: {ingestion_date}
Completed At: {datetime.now()}
{'='*60}
""")

# COMMAND ----------


