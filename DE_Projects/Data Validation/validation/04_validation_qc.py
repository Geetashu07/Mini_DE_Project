# Databricks notebook source
# MAGIC %md
# MAGIC # Validation and QC Framework
# MAGIC
# MAGIC This notebook runs comprehensive data quality checks across all layers.
# MAGIC
# MAGIC **Features:**
# MAGIC - Comprehensive QC checks
# MAGIC - Validation results storage
# MAGIC - QC reporting
# MAGIC - Alert generation
# MAGIC
# MAGIC **Source:** All Bronze, Silver, and Gold tables
# MAGIC **Destination:** `medallion_project.validation.validation_results` table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration Setup

# COMMAND ----------

# Import configuration
import sys
sys.path.append("/Workspace/Repos/your-repo/databricks-medallion-project")

from config.project_config import (
    CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, VALIDATION_SCHEMA,
    BRONZE_CUSTOMERS_TABLE, BRONZE_PRODUCTS_TABLE, 
    BRONZE_STORES_TABLE, BRONZE_TRANSACTIONS_TABLE,
    SILVER_CUSTOMERS_TABLE, SILVER_PRODUCTS_TABLE,
    SILVER_STORES_TABLE, SILVER_TRANSACTIONS_TABLE,
    GOLD_CUSTOMER_SUMMARY_TABLE, GOLD_PRODUCT_SALES_TABLE,
    GOLD_STORE_PERFORMANCE_TABLE, GOLD_DAILY_SALES_TABLE,
    VALIDATION_RESULTS_TABLE,
    get_batch_id, get_ingestion_date
)

from utils.qc_framework import (
    check_row_count, check_nulls, check_duplicates,
    check_referential_integrity, check_value_range,
    check_valid_values, run_qc_checks, save_qc_results, QCResult
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, max as spark_max, min as spark_min,
    avg, sum as spark_sum, current_timestamp
)
from datetime import datetime
import time

# Initialize Spark session
spark = SparkSession.builder.appName("Validation_QC").getOrCreate()

# Get batch ID for this run
batch_id = get_batch_id()
ingestion_date = get_ingestion_date()

print(f"Starting Validation QC - Batch ID: {batch_id}")
print(f"Ingestion Date: {ingestion_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Validation Schema

# COMMAND ----------

# Use the catalog (schemas already exist)
spark.sql(f"USE CATALOG {CATALOG_NAME}")

# Create validation results table if not exists (in default schema)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {VALIDATION_RESULTS_TABLE} (
        table_name STRING,
        check_name STRING,
        status STRING,
        message STRING,
        row_count INT,
        failed_rows INT,
        threshold DOUBLE,
        actual_value DOUBLE,
        timestamp TIMESTAMP,
        batch_id STRING
    )
    USING DELTA
""")

print(f"Created validation results table: {VALIDATION_RESULTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate Bronze Layer

# COMMAND ----------

def validate_bronze_layer():
    """Validate Bronze layer tables"""
    print(f"\n{'='*60}")
    print("Validating Bronze Layer")
    print(f"{'='*60}")
    
    all_results = []
    
    # Validate Customers
    print("\nValidating Bronze Customers...")
    bronze_customers = spark.table(BRONZE_CUSTOMERS_TABLE)
    customers_qc = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["customer_id"],
            "max_null_percentage": 0.0
        }
    }
    customers_results = run_qc_checks(bronze_customers, BRONZE_CUSTOMERS_TABLE, customers_qc)
    all_results.extend(customers_results)
    
    # Validate Products
    print("\nValidating Bronze Products...")
    bronze_products = spark.table(BRONZE_PRODUCTS_TABLE)
    products_qc = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["sku"],
            "max_null_percentage": 0.0
        }
    }
    products_results = run_qc_checks(bronze_products, BRONZE_PRODUCTS_TABLE, products_qc)
    all_results.extend(products_results)
    
    # Validate Stores
    print("\nValidating Bronze Stores...")
    bronze_stores = spark.table(BRONZE_STORES_TABLE)
    stores_qc = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["store_id"],
            "max_null_percentage": 0.0
        }
    }
    stores_results = run_qc_checks(bronze_stores, BRONZE_STORES_TABLE, stores_qc)
    all_results.extend(stores_results)
    
    # Validate Transactions
    print("\nValidating Bronze Transactions...")
    bronze_transactions = spark.table(BRONZE_TRANSACTIONS_TABLE)
    transactions_qc = {
        "row_count": {"min_rows": 1},
        "nulls": {
            "columns": ["transaction_id", "sku"],
            "max_null_percentage": 0.0
        }
    }
    transactions_results = run_qc_checks(bronze_transactions, BRONZE_TRANSACTIONS_TABLE, transactions_qc)
    all_results.extend(transactions_results)
    
    return all_results

# COMMAND ----------

bronze_results = validate_bronze_layer()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate Silver Layer

# COMMAND ----------

def validate_silver_layer():
    """Validate Silver layer tables"""
    print(f"\n{'='*60}")
    print("Validating Silver Layer")
    print(f"{'='*60}")
    
    all_results = []
    
    # Validate Customers
    print("\nValidating Silver Customers...")
    silver_customers = spark.table(SILVER_CUSTOMERS_TABLE)
    customers_qc = {
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
    customers_results = run_qc_checks(silver_customers, SILVER_CUSTOMERS_TABLE, customers_qc)
    all_results.extend(customers_results)
    
    # Validate Products
    print("\nValidating Silver Products...")
    silver_products = spark.table(SILVER_PRODUCTS_TABLE)
    products_qc = {
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
            "list_price": {"min_value": 0.0, "max_value": 10000.0}
        }
    }
    products_results = run_qc_checks(silver_products, SILVER_PRODUCTS_TABLE, products_qc)
    all_results.extend(products_results)
    
    # Validate Stores
    print("\nValidating Silver Stores...")
    silver_stores = spark.table(SILVER_STORES_TABLE)
    stores_qc = {
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
    stores_results = run_qc_checks(silver_stores, SILVER_STORES_TABLE, stores_qc)
    all_results.extend(stores_results)
    
    # Validate Transactions
    print("\nValidating Silver Transactions...")
    silver_transactions = spark.table(SILVER_TRANSACTIONS_TABLE)
    transactions_qc = {
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
            "quantity": {"min_value": 1, "max_value": 1000},
            "unit_price": {"min_value": 0.0, "max_value": 10000.0}
        },
        "valid_values": {
            "channel": ["ECOM", "POS"],
            "currency": ["INR"]
        }
    }
    transactions_results = run_qc_checks(silver_transactions, SILVER_TRANSACTIONS_TABLE, transactions_qc)
    all_results.extend(transactions_results)
    
    # Referential integrity checks
    print("\nRunning referential integrity checks...")
    
    # Check customer_id
    ri_customer = check_referential_integrity(
        silver_transactions.filter(col("customer_id").isNotNull()),
        SILVER_TRANSACTIONS_TABLE,
        "customer_id",
        silver_customers,
        "customer_id"
    )
    all_results.append(ri_customer)
    
    # Check store_id
    ri_store = check_referential_integrity(
        silver_transactions.filter(col("store_id").isNotNull()),
        SILVER_TRANSACTIONS_TABLE,
        "store_id",
        silver_stores,
        "store_id"
    )
    all_results.append(ri_store)
    
    # Check sku
    ri_product = check_referential_integrity(
        silver_transactions,
        SILVER_TRANSACTIONS_TABLE,
        "sku",
        silver_products,
        "sku"
    )
    all_results.append(ri_product)
    
    return all_results

# COMMAND ----------

silver_results = validate_silver_layer()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Gold Layer

# COMMAND ----------

def validate_gold_layer():
    """Validate Gold layer tables"""
    print(f"\n{'='*60}")
    print("Validating Gold Layer")
    print(f"{'='*60}")
    
    all_results = []
    
    # Validate Customer Summary
    print("\nValidating Gold Customer Summary...")
    gold_customers = spark.table(GOLD_CUSTOMER_SUMMARY_TABLE)
    customers_qc = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_sales": {"min_value": 0.0},
            "total_transactions": {"min_value": 0}
        }
    }
    customers_results = run_qc_checks(gold_customers, GOLD_CUSTOMER_SUMMARY_TABLE, customers_qc)
    all_results.extend(customers_results)
    
    # Validate Product Sales
    print("\nValidating Gold Product Sales...")
    gold_products = spark.table(GOLD_PRODUCT_SALES_TABLE)
    products_qc = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_revenue": {"min_value": 0.0},
            "total_quantity_sold": {"min_value": 0}
        }
    }
    products_results = run_qc_checks(gold_products, GOLD_PRODUCT_SALES_TABLE, products_qc)
    all_results.extend(products_results)
    
    # Validate Store Performance
    print("\nValidating Gold Store Performance...")
    gold_stores = spark.table(GOLD_STORE_PERFORMANCE_TABLE)
    stores_qc = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_revenue": {"min_value": 0.0},
            "total_transactions": {"min_value": 0}
        }
    }
    stores_results = run_qc_checks(gold_stores, GOLD_STORE_PERFORMANCE_TABLE, stores_qc)
    all_results.extend(stores_results)
    
    # Validate Daily Sales
    print("\nValidating Gold Daily Sales...")
    gold_daily = spark.table(GOLD_DAILY_SALES_TABLE)
    daily_qc = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_revenue": {"min_value": 0.0},
            "total_transactions": {"min_value": 0}
        }
    }
    daily_results = run_qc_checks(gold_daily, GOLD_DAILY_SALES_TABLE, daily_qc)
    all_results.extend(daily_results)
    
    # Business rule: Total sales should be positive
    print("\nValidating business rules...")
    
    # Check if total sales > 0
    total_sales = gold_customers.agg(spark_sum("total_sales").alias("total")).collect()[0]["total"]
    if total_sales <= 0:
        all_results.append(QCResult(
            table_name=GOLD_CUSTOMER_SUMMARY_TABLE,
            check_name="business_rule_total_sales",
            status="FAIL",
            message=f"Total sales {total_sales} is not positive"
        ))
    else:
        all_results.append(QCResult(
            table_name=GOLD_CUSTOMER_SUMMARY_TABLE,
            check_name="business_rule_total_sales",
            status="PASS",
            message=f"Total sales {total_sales} is positive"
        ))
    
    return all_results

# COMMAND ----------

gold_results = validate_gold_layer()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Generate Validation Report

# COMMAND ----------

# Collect all results
all_results = bronze_results + silver_results + gold_results

# Save results
save_qc_results(spark, all_results, VALIDATION_RESULTS_TABLE, batch_id)

# Generate summary
print(f"\n{'='*60}")
print("Validation Summary")
print(f"{'='*60}")

total_checks = len(all_results)
passed_checks = len([r for r in all_results if r.status == "PASS"])
failed_checks = len([r for r in all_results if r.status == "FAIL"])
warning_checks = len([r for r in all_results if r.status == "WARNING"])

print(f"Total Checks: {total_checks}")
print(f"Passed: {passed_checks} ({passed_checks/total_checks*100:.1f}%)")
print(f"Failed: {failed_checks} ({failed_checks/total_checks*100:.1f}%)")
print(f"Warnings: {warning_checks} ({warning_checks/total_checks*100:.1f}%)")

# Print failed checks
if failed_checks > 0:
    print(f"\n{'='*60}")
    print("Failed Checks:")
    print(f"{'='*60}")
    for result in all_results:
        if result.status == "FAIL":
            print(f"Table: {result.table_name}")
            print(f"Check: {result.check_name}")
            print(f"Message: {result.message}")
            print()

# Print warning checks
if warning_checks > 0:
    print(f"\n{'='*60}")
    print("Warning Checks:")
    print(f"{'='*60}")
    for result in all_results:
        if result.status == "WARNING":
            print(f"Table: {result.table_name}")
            print(f"Check: {result.check_name}")
            print(f"Message: {result.message}")
            print()

print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validation Results Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Latest validation results
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     check_name,
# MAGIC     status,
# MAGIC     message,
# MAGIC     row_count,
# MAGIC     failed_rows,
# MAGIC     timestamp,
# MAGIC     batch_id
# MAGIC FROM medallion_project.validation.validation_results
# MAGIC WHERE batch_id = '${batch_id}'
# MAGIC ORDER BY table_name, check_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation summary by status
# MAGIC SELECT 
# MAGIC     status,
# MAGIC     COUNT(*) as check_count,
# MAGIC     COUNT(DISTINCT table_name) as table_count
# MAGIC FROM medallion_project.validation.validation_results
# MAGIC WHERE batch_id = '${batch_id}'
# MAGIC GROUP BY status
# MAGIC ORDER BY status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation summary by table
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     COUNT(*) as total_checks,
# MAGIC     SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
# MAGIC     SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
# MAGIC     SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) as warnings
# MAGIC FROM medallion_project.validation.validation_results
# MAGIC WHERE batch_id = '${batch_id}'
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Summary

# COMMAND ----------

print(f"""
Validation QC Completed
{'='*60}
Batch ID: {batch_id}
Ingestion Date: {ingestion_date}
Completed At: {datetime.now()}
Total Checks: {total_checks}
Passed: {passed_checks}
Failed: {failed_checks}
Warnings: {warning_checks}
{'='*60}
""")

# Raise exception if any checks failed
if failed_checks > 0:
    raise Exception(f"Validation failed with {failed_checks} failed checks")

# COMMAND ----------


