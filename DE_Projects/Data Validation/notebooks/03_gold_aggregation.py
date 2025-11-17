# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregation and Enrichment
# MAGIC
# MAGIC This notebook handles business-level aggregations and enrichment.
# MAGIC
# MAGIC **Features:**
# MAGIC - Business-level aggregations
# MAGIC - Enriched dimensions
# MAGIC - Business rule validations
# MAGIC - Ready for reporting/analytics
# MAGIC
# MAGIC **Source:** `medallion_project.silver.*` tables
# MAGIC **Destination:** `medallion_project.gold.*` tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration Setup

# COMMAND ----------

# Import configuration
import sys
sys.path.append('/Workspace/Users/negigeetanshusingh@gmail.com/Mini_DE_Project/DE_Projects/Data Validation/config')
sys.path.append('/Workspace/Users/negigeetanshusingh@gmail.com/Mini_DE_Project/DE_Projects/Data Validation/utils')

from project_config import *

from project_config import (
    CATALOG_NAME, SILVER_SCHEMA, GOLD_SCHEMA,
    SILVER_CUSTOMERS_TABLE, SILVER_PRODUCTS_TABLE,
    SILVER_STORES_TABLE, SILVER_TRANSACTIONS_TABLE,
    GOLD_CUSTOMER_SUMMARY_TABLE, GOLD_PRODUCT_SALES_TABLE,
    GOLD_STORE_PERFORMANCE_TABLE, GOLD_DAILY_SALES_TABLE,
    get_batch_id, get_ingestion_date
)

from qc_framework import (
    check_row_count, check_value_range, run_qc_checks, save_qc_results
)

from helpers import (
     add_audit_columns
)


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sum as spark_sum, count, countDistinct, avg, max as spark_max,
    min as spark_min, when, isnull, coalesce, date_format, round, current_timestamp
)
from datetime import datetime
import time

# Initialize Spark session
spark = SparkSession.builder.appName("Gold_Aggregation").getOrCreate()

# Get batch ID for this run
batch_id = get_batch_id()
ingestion_date = get_ingestion_date()

print(f"Starting Gold Aggregation - Batch ID: {batch_id}")
print(f"Ingestion Date: {ingestion_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Gold Schema

# COMMAND ----------

# Use the catalog (schemas already exist)
spark.sql(f"USE CATALOG {CATALOG_NAME}")

print(f"Created schema: {CATALOG_NAME}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Customer Summary

# COMMAND ----------

def create_customer_summary():
    """Create customer summary with transaction statistics"""
    print(f"\n{'='*60}")
    print("Creating Customer Summary")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Read from Silver
    customers_df = spark.table(SILVER_CUSTOMERS_TABLE)
    transactions_df = spark.table(SILVER_TRANSACTIONS_TABLE)
    
    # Calculate customer transaction statistics
    customer_stats = transactions_df \
        .filter(col("customer_id").isNotNull()) \
        .groupBy("customer_id") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            countDistinct("transaction_id").alias("unique_transactions"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum((col("unit_price") * col("quantity") * (1 - col("discount") / 100))).alias("total_sales"),
            spark_sum(col("discount") * col("unit_price") * col("quantity") / 100).alias("total_discount"),
            avg(col("unit_price")).alias("avg_unit_price"),
            spark_max("transaction_ts").alias("last_transaction_date")
        ) \
        .withColumn("total_sales", round(col("total_sales"), 2)) \
        .withColumn("total_discount", round(col("total_discount"), 2)) \
        .withColumn("avg_unit_price", round(col("avg_unit_price"), 2))
    
    # Join with customers to get customer details
    customer_summary = customers_df \
        .join(customer_stats, "customer_id", "left") \
        .select(
            customers_df["customer_id"],
            customers_df["name"],
            customers_df["signup_date"],
            coalesce(customer_stats["total_transactions"], lit(0)).alias("total_transactions"),
            coalesce(customer_stats["unique_transactions"], lit(0)).alias("unique_transactions"),
            coalesce(customer_stats["total_quantity"], lit(0)).alias("total_quantity"),
            coalesce(customer_stats["total_sales"], lit(0.0)).alias("total_sales"),
            coalesce(customer_stats["total_discount"], lit(0.0)).alias("total_discount"),
            coalesce(customer_stats["avg_unit_price"], lit(0.0)).alias("avg_unit_price"),
            customer_stats["last_transaction_date"],
            current_timestamp().alias("_updated_at"),
            customers_df["_record_ingestion_ts"])
        
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_sales": {"min_value": 0.0},
            "total_transactions": {"min_value": 0}
        }
    }
    qc_results = run_qc_checks(customer_summary, GOLD_CUSTOMER_SUMMARY_TABLE, qc_checks)
    
    # Print QC results
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Write to Gold
    print(f"\nWriting to {GOLD_CUSTOMER_SUMMARY_TABLE}...")
    customer_summary.display()
    # Add audit columns
    customer_summary = add_audit_columns(customer_summary, batch_id)
    customer_summary.display()
    customer_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_CUSTOMER_SUMMARY_TABLE)
    
    row_count = customer_summary.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully created {row_count} customer summaries")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------

customer_summary_qc_results = create_customer_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Product Sales

# COMMAND ----------

def create_product_sales():
    """Create product sales summary"""
    print(f"\n{'='*60}")
    print("Creating Product Sales")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Read from Silver
    products_df = spark.table(SILVER_PRODUCTS_TABLE)
    transactions_df = spark.table(SILVER_TRANSACTIONS_TABLE)
    
    # Calculate product sales statistics
    product_stats = transactions_df \
        .groupBy("sku") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            countDistinct("transaction_id").alias("unique_transactions"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum((col("unit_price") * col("quantity") * (1 - col("discount") / 100))).alias("total_revenue"),
            spark_sum(col("discount") * col("unit_price") * col("quantity") / 100).alias("total_discount"),
            avg(col("unit_price")).alias("avg_selling_price"),
            spark_min("unit_price").alias("min_selling_price"),
            spark_max("unit_price").alias("max_selling_price"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("store_id").alias("unique_stores")
        ) \
        .withColumn("total_revenue", round(col("total_revenue"), 2)) \
        .withColumn("total_discount", round(col("total_discount"), 2)) \
        .withColumn("avg_selling_price", round(col("avg_selling_price"), 2)) \
        .withColumn("min_selling_price", round(col("min_selling_price"), 2)) \
        .withColumn("max_selling_price", round(col("max_selling_price"), 2))
    
    # Join with products to get product details
    product_sales = products_df \
        .join(product_stats, "sku", "left") \
        .select(
            products_df["sku"],
            products_df["product_key"],
            products_df["category"],
            products_df["list_price"],
            coalesce(product_stats["total_transactions"], lit(0)).alias("total_transactions"),
            coalesce(product_stats["unique_transactions"], lit(0)).alias("unique_transactions"),
            coalesce(product_stats["total_quantity_sold"], lit(0)).alias("total_quantity_sold"),
            coalesce(product_stats["total_revenue"], lit(0.0)).alias("total_revenue"),
            coalesce(product_stats["total_discount"], lit(0.0)).alias("total_discount"),
            coalesce(product_stats["avg_selling_price"], lit(0.0)).alias("avg_selling_price"),
            coalesce(product_stats["min_selling_price"], lit(0.0)).alias("min_selling_price"),
            coalesce(product_stats["max_selling_price"], lit(0.0)).alias("max_selling_price"),
            coalesce(product_stats["unique_customers"], lit(0)).alias("unique_customers"),
            coalesce(product_stats["unique_stores"], lit(0)).alias("unique_stores"),
            current_timestamp().alias("_updated_at"),
            products_df["_record_ingestion_ts"]
        )
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_revenue": {"min_value": 0.0},
            "total_quantity_sold": {"min_value": 0}
        }
    }
    qc_results = run_qc_checks(product_sales, GOLD_PRODUCT_SALES_TABLE, qc_checks)
    
    # Print QC results
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Add audit columns
    product_sales = add_audit_columns(product_sales, batch_id)
    # Write to Gold
    print(f"\nWriting to {GOLD_PRODUCT_SALES_TABLE}...")
    product_sales.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(GOLD_PRODUCT_SALES_TABLE)
    
    row_count = product_sales.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully created {row_count} product sales records")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------

product_sales_qc_results = create_product_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Store Performance

# COMMAND ----------

def create_store_performance():
    """Create store performance summary"""
    print(f"\n{'='*60}")
    print("Creating Store Performance")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Read from Silver
    stores_df = spark.table(SILVER_STORES_TABLE)
    transactions_df = spark.table(SILVER_TRANSACTIONS_TABLE)
    
    # Calculate store performance statistics
    store_stats = transactions_df \
        .filter(col("store_id").isNotNull()) \
        .groupBy("store_id") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            countDistinct("transaction_id").alias("unique_transactions"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("sku").alias("unique_products"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum((col("unit_price") * col("quantity") * (1 - col("discount") / 100))).alias("total_revenue"),
            spark_sum(col("discount") * col("unit_price") * col("quantity") / 100).alias("total_discount"),
            avg(col("unit_price") * col("quantity") * (1 - col("discount") / 100)).alias("avg_transaction_value"),
            spark_max("transaction_ts").alias("last_transaction_date")
        ) \
        .withColumn("total_revenue", round(col("total_revenue"), 2)) \
        .withColumn("total_discount", round(col("total_discount"), 2)) \
        .withColumn("avg_transaction_value", round(col("avg_transaction_value"), 2))
    
    # Join with stores to get store details
    store_performance = stores_df \
        .join(store_stats, "store_id", "left") \
        .select(
            stores_df["store_id"],
            stores_df["name"],
            stores_df["region"],
            coalesce(store_stats["total_transactions"], lit(0)).alias("total_transactions"),
            coalesce(store_stats["unique_transactions"], lit(0)).alias("unique_transactions"),
            coalesce(store_stats["unique_customers"], lit(0)).alias("unique_customers"),
            coalesce(store_stats["unique_products"], lit(0)).alias("unique_products"),
            coalesce(store_stats["total_quantity_sold"], lit(0)).alias("total_quantity_sold"),
            coalesce(store_stats["total_revenue"], lit(0.0)).alias("total_revenue"),
            coalesce(store_stats["total_discount"], lit(0.0)).alias("total_discount"),
            coalesce(store_stats["avg_transaction_value"], lit(0.0)).alias("avg_transaction_value"),
            store_stats["last_transaction_date"],
            current_timestamp().alias("_updated_at"),
            stores_df["_record_ingestion_ts"]
        )
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_revenue": {"min_value": 0.0},
            "total_transactions": {"min_value": 0}
        }
    }
    qc_results = run_qc_checks(store_performance, GOLD_STORE_PERFORMANCE_TABLE, qc_checks)
    
    # Print QC results
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Add audit columns
    store_performance = add_audit_columns(store_performance, batch_id)
    # Write to Gold
    print(f"\nWriting to {GOLD_STORE_PERFORMANCE_TABLE}...")
    store_performance.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(GOLD_STORE_PERFORMANCE_TABLE)
    
    row_count = store_performance.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully created {row_count} store performance records")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------

store_performance_qc_results = create_store_performance()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Daily Sales

# COMMAND ----------

def create_daily_sales():
    """Create daily sales summary"""
    print(f"\n{'='*60}")
    print("Creating Daily Sales")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Read from Silver
    transactions_df = spark.table(SILVER_TRANSACTIONS_TABLE)
    
    # Calculate daily sales
    daily_sales = transactions_df \
        .withColumn("sale_date", date_format(col("transaction_ts"), "yyyy-MM-dd")) \
        .groupBy("sale_date","_record_ingestion_ts") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            countDistinct("transaction_id").alias("unique_transactions"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("store_id").alias("unique_stores"),
            countDistinct("sku").alias("unique_products"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum((col("unit_price") * col("quantity") * (1 - col("discount") / 100))).alias("total_revenue"),
            spark_sum(col("discount") * col("unit_price") * col("quantity") / 100).alias("total_discount"),
            avg(col("unit_price") * col("quantity") * (1 - col("discount") / 100)).alias("avg_transaction_value"),
            spark_max("transaction_ts").alias("last_transaction_time")
        ) \
        .withColumn("total_revenue", round(col("total_revenue"), 2)) \
        .withColumn("total_discount", round(col("total_discount"), 2)) \
        .withColumn("avg_transaction_value", round(col("avg_transaction_value"), 2)) \
        .withColumn("sale_date", col("sale_date").cast("date")) \
        .withColumn("_updated_at", current_timestamp()) \
        .orderBy("sale_date")
    
    # QC checks
    print("\nRunning QC checks...")
    qc_checks = {
        "row_count": {"min_rows": 1},
        "value_ranges": {
            "total_revenue": {"min_value": 0.0},
            "total_transactions": {"min_value": 0}
        }
    }
    qc_results = run_qc_checks(daily_sales, GOLD_DAILY_SALES_TABLE, qc_checks)
    
    # Print QC results
    for result in qc_results:
        print(f"  {result.check_name}: {result.status} - {result.message}")
        if result.status == "FAIL":
            raise Exception(f"QC check failed: {result.message}")
    
    # Add audit columns
    daily_sales = add_audit_columns(daily_sales, batch_id)
    #daily_sales.display()
    # Write to Gold
    print(f"\nWriting to {GOLD_DAILY_SALES_TABLE}...")
    daily_sales.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(GOLD_DAILY_SALES_TABLE)
    
    row_count = daily_sales.count()
    processing_time = time.time() - start_time
    
    print(f"Successfully created {row_count} daily sales records")
    print(f"Processing time: {processing_time:.2f} seconds")
    
    return qc_results

# COMMAND ----------

daily_sales_qc_results = create_daily_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Verify Aggregation

# COMMAND ----------

# Verify row counts
print("Gold Layer Row Counts:")
print("=" * 60)

tables = [
    ("Customer Summary", GOLD_CUSTOMER_SUMMARY_TABLE),
    ("Product Sales", GOLD_PRODUCT_SALES_TABLE),
    ("Store Performance", GOLD_STORE_PERFORMANCE_TABLE),
    ("Daily Sales", GOLD_DAILY_SALES_TABLE)
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
# MAGIC ## Step 8: Summary

# COMMAND ----------

# # Collect all QC results
# all_qc_results = (
#     customer_summary_qc_results + 
#     product_sales_qc_results + 
#     store_performance_qc_results + 
#     daily_sales_qc_results
# )

# # Save QC results
# from project_config import VALIDATION_RESULTS_TABLE
# save_qc_results(spark, all_qc_results, VALIDATION_RESULTS_TABLE, batch_id)

# print(f"""
# Gold Aggregation Completed
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

# MAGIC %md
# MAGIC ## Step 9: Sample Queries

# COMMAND ----------

# %sql
# -- Top 10 customers by total sales
# SELECT 
#     customer_id,
#     name,
#     total_sales,
#     total_transactions,
#     total_quantity
# FROM medallion_project.gold.gold_customer_summary
# ORDER BY total_sales DESC
# LIMIT 10

# COMMAND ----------

# %sql
# -- Top 10 products by revenue
# SELECT 
#     sku,
#     category,
#     total_revenue,
#     total_quantity_sold,
#     unique_customers
# FROM medallion_project.gold.gold_product_sales
# ORDER BY total_revenue DESC
# LIMIT 10

# COMMAND ----------

# %sql
# -- Store performance by region
# SELECT 
#     region,
#     COUNT(*) as store_count,
#     SUM(total_revenue) as total_revenue,
#     SUM(total_transactions) as total_transactions,
#     AVG(avg_transaction_value) as avg_transaction_value
# FROM medallion_project.gold.gold_store_performance
# GROUP BY region
# ORDER BY total_revenue DESC

# COMMAND ----------

# %sql
# -- Daily sales trend
# SELECT 
#     sale_date,
#     total_revenue,
#     total_transactions,
#     unique_customers,
#     avg_transaction_value
# FROM medallion_project.gold.gold_daily_sales
# ORDER BY sale_date DESC
# LIMIT 30

# COMMAND ----------


