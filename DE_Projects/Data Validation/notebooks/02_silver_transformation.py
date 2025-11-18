# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Cleaning and Transformation
# MAGIC
# MAGIC This notebook handles data cleaning and standardization.
# MAGIC
# MAGIC **Features:**
# MAGIC - Data cleaning (trim strings, handle nulls)
# MAGIC - Data type corrections
# MAGIC - Referential integrity checks
# MAGIC - Deduplication
# MAGIC - QC checks (nulls, duplicates, transformations)
# MAGIC
# MAGIC **Source:** `medallion_project.bronze.*` tables
# MAGIC **Destination:** `medallion_project.silver.*` tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration Setup

# COMMAND ----------

# Import configuration
import sys
sys.path.append('/Workspace/Users/negigeetanshusingh@gmail.com/Mini_DE_Project/DE_Projects/Data Validation/config')
sys.path.append('/Workspace/Users/negigeetanshusingh@gmail.com/Mini_DE_Project/DE_Projects/Data Validation/utils')

from project_config import *
from delta.tables import DeltaTable

from project_config import (
    CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA,
    BRONZE_CUSTOMERS_TABLE, BRONZE_PRODUCTS_TABLE, 
    BRONZE_STORES_TABLE, BRONZE_TRANSACTIONS_TABLE,
    SILVER_CUSTOMERS_TABLE, SILVER_PRODUCTS_TABLE,
    SILVER_STORES_TABLE, SILVER_TRANSACTIONS_TABLE,
    get_batch_id, get_ingestion_date,
    VALIDATION_RULES, VALID_CHANNELS, VALID_CURRENCY,
    MIN_PRICE, MAX_PRICE, MIN_QUANTITY, MAX_QUANTITY
)

from helpers import (
    clean_string_columns, standardize_string_case,
    handle_nulls, remove_duplicates, add_audit_columns,
    convert_data_types, merge_data,load_audit
)

from qc_framework import (
    check_row_count, check_nulls, check_duplicates,
    check_referential_integrity, check_value_range,
    check_valid_values, run_qc_checks, save_qc_results
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, trim, upper, lower,
    when, isnull, coalesce, regexp_replace, date_format
)
from datetime import datetime
import time

# Initialize Spark session
spark = SparkSession.builder.appName("Silver_Transformation").getOrCreate()

# Get batch ID for this run
batch_id = get_batch_id()
ingestion_date = get_ingestion_date()

print(f"Starting Silver Transformation - Batch ID: {batch_id}")
print(f"Ingestion Date: {ingestion_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Silver Schema

# COMMAND ----------

# Use the catalog (schemas already exist)
spark.sql(f"USE CATALOG {CATALOG_NAME}")

print(f"Created schema: {CATALOG_NAME}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transform Customers

# COMMAND ----------

def transform_customers():
    """Transform customers from Bronze to Silver"""
    print(f"\n{'='*60}")
    print("Transforming Customers")
    print(f"{'='*60}")
    
    start_time = time.time()
    start_ts = datetime.now()
    
    
    # Read from Bronze
    if spark.catalog.tableExists('qc_validation.silver.silver_customers'):
        bronze_df=spark.sql("""SELECT *
        FROM qc_validation.bronze.bronze_customers
        WHERE _record_ingestion_ts > COALESCE(
            (SELECT MAX(log_ts) FROM qc_validation.default.load_audit where table_name='qc_validation.silver.silver_customers'
),
            TIMESTAMP('1900-01-01 00:00:00')
        )""")
        print(f"Read {bronze_df.count()} rows from Bronze")
    else:
        bronze_df=spark.sql(f"select * from qc_validation.bronze.bronze_customers")
    
    # Clean string columns
    silver_df = clean_string_columns(bronze_df, ["customer_id", "name"])
    
    # Standardize case
    silver_df = standardize_string_case(silver_df, ["customer_id"], "upper")
    
    # Handle nulls
    silver_df = handle_nulls(silver_df, {
        "customer_id": None,  # Required field - will fail QC if null
        "name": "Unknown"
    })
    
    # Remove duplicates (keep latest)
    silver_df = remove_duplicates(silver_df, ["customer_id"], "first")
    
    # Add audit columns
    silver_df = add_audit_columns(silver_df, batch_id)
    
    # Remove old audit columns if they exist
    if "_source_file" in silver_df.columns:
        silver_df = silver_df.drop("_source_file")
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["customer_id", "name"],
            "max_null_percentage": 0.0
        },
        "duplicates": {
            "key_columns": ["customer_id"],
            "max_duplicate_percentage": 0.0
        }
    }
    qc_results = run_qc_checks(silver_df, SILVER_CUSTOMERS_TABLE, qc_checks)
    
    # Print QC results
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Write to Silver using Delta Lake merge
    print(f"\nWriting to {SILVER_CUSTOMERS_TABLE}...")
    print(silver_df.count())
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists('qc_validation.silver.silver_customers'):
        silver_df.write.format("delta").saveAsTable('qc_validation.silver.silver_customers')
    else:
        (
            DeltaTable.forName(spark, 'qc_validation.silver.silver_customers')
            .alias("final")
            .merge(
                source=silver_df.alias("incr"),
                condition="final.customer_id = incr.customer_id" 
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    # Track load audit for both initial and incremental loads
    if silver_df.count() > 0:
        end_time = time.time()
        end_ts = datetime.now()
        max_df_time = silver_df.agg({"_record_ingestion_ts": "max"}).collect()[0][0]
        load_audit(spark,
                silver_df,
                batch_id,
                'qc_validation.silver.silver_customers',
                max_df_time,
                start_ts,
                end_ts)
        
    row_count = silver_df.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully transformed {row_count} rows")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------

customers_qc_results = transform_customers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Transform Products

# COMMAND ----------

def transform_products():
    """Transform products from Bronze to Silver"""
    print(f"\n{'='*60}")
    print("Transforming Products")
    print(f"{'='*60}")
    
    start_time = time.time()
    start_ts = datetime.now()
    
    # Read from Bronze with incremental logic using load_audit table
    if spark.catalog.tableExists('qc_validation.silver.silver_products'):
        bronze_df = spark.sql("""SELECT *
        FROM qc_validation.bronze.bronze_products
        WHERE _record_ingestion_ts > COALESCE(
            (SELECT MAX(log_ts) FROM qc_validation.default.load_audit 
             WHERE table_name='qc_validation.silver.silver_products'),
            TIMESTAMP('1900-01-01 00:00:00')
        )""")
        print(f"Read {bronze_df.count()} rows from Bronze (incremental)")
    else:
        bronze_df = spark.sql(f"SELECT * FROM qc_validation.bronze.bronze_products")
        print(f"Read {bronze_df.count()} rows from Bronze (initial load)")
    
    # Clean string columns
    silver_df = clean_string_columns(bronze_df, ["sku", "product_key", "category"])
    
    # Standardize case
    silver_df = standardize_string_case(silver_df, ["sku", "product_key"], "upper")
    
    # Handle nulls
    silver_df = handle_nulls(silver_df, {
        "sku": None,  # Required field
        "product_key": None,  # Required field
        "category": "Unknown",
        "list_price": 0.0
    })
    
    # Remove duplicates (keep latest)
    silver_df = remove_duplicates(silver_df, ["sku"], "first")
    
    # Add audit columns
    silver_df = add_audit_columns(silver_df, batch_id)
    
    # Remove old audit columns if they exist
    if "_source_file" in silver_df.columns:
        silver_df = silver_df.drop("_source_file")
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["sku", "product_key", "list_price"],
            "max_null_percentage": 0.0
        },
        "duplicates": {
            "key_columns": ["sku"],
            "max_duplicate_percentage": 0.0
        },
        "value_ranges": {
            "list_price": {"min_value": MIN_PRICE, "max_value": MAX_PRICE}
        }
    }
    qc_results = run_qc_checks(silver_df, SILVER_PRODUCTS_TABLE, qc_checks)
    
    # Print QC results
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Write to Silver using Delta Lake merge
    print(f"\nWriting to {SILVER_PRODUCTS_TABLE}...")
    print(silver_df.count())
    
    if not spark.catalog.tableExists('qc_validation.silver.silver_products'):
        silver_df.write.format("delta").saveAsTable('qc_validation.silver.silver_products')
    else:
        (
            DeltaTable.forName(spark, 'qc_validation.silver.silver_products')
            .alias("final")
            .merge(
                source=silver_df.alias("incr"),
                condition="final.product_key = incr.product_key"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    # Track load audit for both initial and incremental loads
    if silver_df.count() > 0:
        end_time = time.time()
        end_ts = datetime.now()
        max_df_time = silver_df.agg({"_record_ingestion_ts": "max"}).collect()[0][0]
        load_audit(spark,
                silver_df,
                batch_id,
                'qc_validation.silver.silver_products',
                max_df_time,
                start_ts,
                end_ts)
    
    row_count = silver_df.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully transformed {row_count} rows")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------


products_qc_results = transform_products()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Transform Stores

# COMMAND ----------

def transform_stores():
    """Transform stores from Bronze to Silver"""
    print(f"\n{'='*60}")
    print("Transforming Stores")
    print(f"{'='*60}")
    
    start_time = time.time()
    start_ts = datetime.now()
    
    # Read from Bronze with incremental logic using load_audit table
    if spark.catalog.tableExists('qc_validation.silver.silver_stores'):
        bronze_df = spark.sql("""SELECT *
        FROM qc_validation.bronze.bronze_stores
        WHERE _record_ingestion_ts > COALESCE(
            (SELECT MAX(log_ts) FROM qc_validation.default.load_audit 
             WHERE table_name='qc_validation.silver.silver_stores'),
            TIMESTAMP('1900-01-01 00:00:00')
        )""")
        print(f"Read {bronze_df.count()} rows from Bronze (incremental)")
    else:
        bronze_df = spark.sql(f"SELECT * FROM qc_validation.bronze.bronze_stores")
        print(f"Read {bronze_df.count()} rows from Bronze (initial load)")
    
    # Clean string columns
    silver_df = clean_string_columns(bronze_df, ["store_id", "name", "region"])
    
    # Standardize case
    silver_df = standardize_string_case(silver_df, ["store_id"], "upper")
    
    # Handle nulls
    silver_df = handle_nulls(silver_df, {
        "store_id": None,  # Required field
        "name": "Unknown",
        "region": "Unknown"
    })
    
    # Remove duplicates (keep latest)
    silver_df = remove_duplicates(silver_df, ["store_id"], "first")
    
    # Add audit columns
    silver_df = add_audit_columns(silver_df, batch_id)
    
    # Remove old audit columns if they exist
    if "_source_file" in silver_df.columns:
        silver_df = silver_df.drop("_source_file")
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["store_id", "name", "region"],
            "max_null_percentage": 0.0
        },
        "duplicates": {
            "key_columns": ["store_id"],
            "max_duplicate_percentage": 0.0
        }
    }
    qc_results = run_qc_checks(silver_df, SILVER_STORES_TABLE, qc_checks)
    
    # Print QC results
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Write to Silver using Delta Lake merge
    print(f"\nWriting to {SILVER_STORES_TABLE}...")
    print(silver_df.count())
    
    if not spark.catalog.tableExists('qc_validation.silver.silver_stores'):
        silver_df.write.format("delta").saveAsTable('qc_validation.silver.silver_stores')
    else:
        (
            DeltaTable.forName(spark, 'qc_validation.silver.silver_stores')
            .alias("final")
            .merge(
                source=silver_df.alias("incr"),
                condition="final.store_id = incr.store_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    # Track load audit for both initial and incremental loads
    if silver_df.count() > 0:
        end_time = time.time()
        end_ts = datetime.now()
        max_df_time = silver_df.agg({"_record_ingestion_ts": "max"}).collect()[0][0]
        load_audit(spark,
                silver_df,
                batch_id,
                'qc_validation.silver.silver_stores',
                max_df_time,
                start_ts,
                end_ts)
    
    row_count = silver_df.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully transformed {row_count} rows")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------


stores_qc_results = transform_stores()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Transform Transactions

# COMMAND ----------

def transform_transactions():
    """Transform transactions from Bronze to Silver"""
    print(f"\n{'='*60}")
    print("Transforming Transactions")
    print(f"{'='*60}")
    
    start_time = time.time()
    start_ts = datetime.now()
    
    # Read from Bronze with incremental logic using load_audit table
    if spark.catalog.tableExists('qc_validation.silver.silver_transactions'):
        bronze_df = spark.sql("""SELECT *
        FROM qc_validation.bronze.bronze_transactions
        WHERE _record_ingestion_ts > COALESCE(
            (SELECT MAX(log_ts) FROM qc_validation.default.load_audit 
             WHERE table_name='qc_validation.silver.silver_transactions'),
            TIMESTAMP('1900-01-01 00:00:00')
        )""")
        print(f"Read {bronze_df.count()} rows from Bronze (incremental)")
    else:
        bronze_df = spark.sql(f"SELECT * FROM qc_validation.bronze.bronze_transactions")
        print(f"Read {bronze_df.count()} rows from Bronze (initial load)")
    
    # Clean string columns
    silver_df = clean_string_columns(bronze_df, [
        "transaction_id", "transaction_line_id", "channel",
        "store_id", "customer_id", "sku", "currency"
    ])
    
    # Standardize case
    silver_df = standardize_string_case(silver_df, [
        "transaction_id", "store_id", "customer_id", "sku", "channel", "currency"
    ], "upper")
    
    # Handle nulls
    silver_df = handle_nulls(silver_df, {
        "transaction_id": None,  # Required field
        "sku": None,  # Required field
        "quantity": 1,
        "unit_price": 0.0,
        "discount": 0.0,
        "currency": "INR"
    })
    
    # Remove duplicates (keep latest)
    silver_df = remove_duplicates(silver_df, ["transaction_id", "transaction_line_id"], "first")
    
    # Add audit columns
    silver_df = add_audit_columns(silver_df, batch_id)
    
    # Remove old audit columns if they exist
    if "_source_file" in silver_df.columns:
        silver_df = silver_df.drop("_source_file")
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["transaction_id", "sku", "quantity", "unit_price"],
            "max_null_percentage": 0.0
        },
        "duplicates": {
            "key_columns": ["transaction_id", "transaction_line_id"],
            "max_duplicate_percentage": 0.0
        },
        "value_ranges": {
            "quantity": {"min_value": MIN_QUANTITY, "max_value": MAX_QUANTITY},
            "unit_price": {"min_value": MIN_PRICE, "max_value": MAX_PRICE},
            "discount": {"min_value": 0.0, "max_value": 100.0}
        },
        "valid_values": {
            "channel": VALID_CHANNELS,
            "currency": VALID_CURRENCY
        }
    }
    qc_results = run_qc_checks(silver_df, SILVER_TRANSACTIONS_TABLE, qc_checks)
    
    # Referential integrity checks
    print("\nRunning referential integrity checks...")
    
    # Check customer_id
    silver_customers = spark.table(SILVER_CUSTOMERS_TABLE)
    ri_customer_result = check_referential_integrity(
        silver_df.filter(col("customer_id").isNotNull()),
        SILVER_TRANSACTIONS_TABLE,
        "customer_id",
        silver_customers,
        "customer_id"
    )
    qc_results.append(ri_customer_result)
    print(f"  {ri_customer_result.check_name}: {ri_customer_result.status} - {ri_customer_result.message}")
    
    # Check store_id
    silver_stores = spark.table(SILVER_STORES_TABLE)
    ri_store_result = check_referential_integrity(
        silver_df.filter(col("store_id").isNotNull()),
        SILVER_TRANSACTIONS_TABLE,
        "store_id",
        silver_stores,
        "store_id"
    )
    qc_results.append(ri_store_result)
    print(f"  {ri_store_result.check_name}: {ri_store_result.status} - {ri_store_result.message}")
    
    # Check sku
    silver_products = spark.table(SILVER_PRODUCTS_TABLE)
    ri_product_result = check_referential_integrity(
        silver_df,
        SILVER_TRANSACTIONS_TABLE,
        "sku",
        silver_products,
        "sku"
    )
    qc_results.append(ri_product_result)
    print(f"  {ri_product_result.check_name}: {ri_product_result.status} - {ri_product_result.message}")
    
    # Print all QC results
    print("\nAll QC Results:")
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Write to Silver using Delta Lake merge
    print(f"\nWriting to {SILVER_TRANSACTIONS_TABLE}...")
    print(silver_df.count())
    
    if not spark.catalog.tableExists('qc_validation.silver.silver_transactions'):
        silver_df.write.format("delta").saveAsTable('qc_validation.silver.silver_transactions')
    else:
        (
            DeltaTable.forName(spark, 'qc_validation.silver.silver_transactions')
            .alias("final")
            .merge(
                source=silver_df.alias("incr"),
                condition="final.transaction_id = incr.transaction_id AND final.transaction_line_id = incr.transaction_line_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    # Track load audit for both initial and incremental loads
    if silver_df.count() > 0:
        end_time = time.time()
        end_ts = datetime.now()
        max_df_time = silver_df.agg({"_record_ingestion_ts": "max"}).collect()[0][0]
        load_audit(spark,
                silver_df,
                batch_id,
                'qc_validation.silver.silver_transactions',
                max_df_time,
                start_ts,
                end_ts)
    
    row_count = silver_df.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully transformed {row_count} rows")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------


transactions_qc_results = transform_transactions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Verify Transformation

# COMMAND ----------

# # Verify row counts
# print("Silver Layer Row Counts:")
# print("=" * 60)

# tables = [
#     ("Customers", SILVER_CUSTOMERS_TABLE),
#     ("Products", SILVER_PRODUCTS_TABLE),
#     ("Stores", SILVER_STORES_TABLE),
#     ("Transactions", SILVER_TRANSACTIONS_TABLE)
# ]

# for table_name, table_path in tables:
#     try:
#         count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_path}").collect()[0]["cnt"]
#         print(f"{table_name:20s}: {count:>10,} rows")
#     except Exception as e:
#         print(f"{table_name:20s}: ERROR - {str(e)}")

# print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Summary

# COMMAND ----------

# # Collect all QC results
# all_qc_results = customers_qc_results + products_qc_results + stores_qc_results + transactions_qc_results

# # Save QC results
# from project_config import VALIDATION_RESULTS_TABLE
# save_qc_results(spark, all_qc_results, VALIDATION_RESULTS_TABLE, batch_id)

# print(f"""
# Silver Transformation Completed
# {'='*60}
# Batch ID: {batch_id}
# Ingestion Date: {ingestion_date}
# Completed At: {datetime.now()}
# Total QC Checks: {len(all_qc_results)}
# Passed: {len([r for r in all_qc_results if r.status == 'PASS'])}
# Failed: {len([r for r in all_qc_results if r.status == 'FAIL'])}
# Warnings: {len([r for r in all_qc_results if r.status == 'WARNING'])}
# {'='*60}
# """)

# COMMAND ----------


