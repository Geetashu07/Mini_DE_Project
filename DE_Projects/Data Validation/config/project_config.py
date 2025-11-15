"""
Project Configuration for Databricks Medallion Architecture

This module contains all configuration settings for the project including
catalog names, schema names, table names, paths, and QC thresholds.
"""

# ============================================================================
# Unity Catalog Configuration
# ============================================================================

# Catalog name
CATALOG_NAME = "qc_validation"

# Schema names for each layer
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
VALIDATION_SCHEMA = "default"  # Using default schema for validation results

# ============================================================================
# Unity Catalog Volume Configuration
# ============================================================================

# Volume path (update with your actual catalog and schema)
VOLUME_CATALOG = "qc_validation"  # Your catalog name
VOLUME_SCHEMA = "raw_files"  # Schema where CSV files are uploaded as volume
VOLUME_NAME = "data"  # Volume name (you'll need to create this)

# Full volume path
VOLUME_PATH = f"/Volumes/{VOLUME_CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}"

# Source data directory within volume
# If files are directly in the volume root, use VOLUME_PATH
# If files are in subdirectories, specify the path here
BRONZE_SOURCE_PATH = VOLUME_PATH  # Files are in the volume root

# ============================================================================
# Table Names
# ============================================================================

# Bronze layer tables
BRONZE_CUSTOMERS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_customers"
BRONZE_PRODUCTS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_products"
BRONZE_STORES_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_stores"
BRONZE_TRANSACTIONS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_transactions"

# Silver layer tables
SILVER_CUSTOMERS_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_customers"
SILVER_PRODUCTS_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_products"
SILVER_STORES_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_stores"
SILVER_TRANSACTIONS_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_transactions"

# Gold layer tables
GOLD_CUSTOMER_SUMMARY_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_customer_summary"
GOLD_PRODUCT_SALES_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_product_sales"
GOLD_STORE_PERFORMANCE_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_store_performance"
GOLD_DAILY_SALES_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_daily_sales"

# Validation table
VALIDATION_RESULTS_TABLE = f"{CATALOG_NAME}.{VALIDATION_SCHEMA}.validation_results"

# Processed files tracking table (in bronze schema)
PROCESSED_FILES_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.processed_files"

# ============================================================================
# File Patterns
# ============================================================================

# File patterns for each entity
CUSTOMERS_FILE_PATTERN = "customers"
PRODUCTS_FILE_PATTERN = "products"
STORES_FILE_PATTERN = "stores"
TRANSACTIONS_FILE_PATTERN = "transactions"

# ============================================================================
# Data Quality Thresholds
# ============================================================================

# Minimum row count threshold (fail if below)
MIN_ROW_COUNT = 1

# Maximum null percentage allowed
MAX_NULL_PERCENTAGE = 0.1  # 10%

# Maximum duplicate percentage allowed
MAX_DUPLICATE_PERCENTAGE = 0.05  # 5%

# ============================================================================
# Processing Configuration
# ============================================================================

# Batch ID (can be set dynamically or from job parameters)
import uuid
from datetime import datetime

def get_batch_id():
    """Generate a unique batch ID for the current run"""
    return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

def get_ingestion_date():
    """Get the current ingestion date"""
    return datetime.now().date()

# ============================================================================
# Schema Definitions
# ============================================================================

# Bronze schemas (as read from CSV)
BRONZE_CUSTOMERS_SCHEMA = """
    customer_id STRING,
    name STRING,
    signup_date DATE
"""

BRONZE_PRODUCTS_SCHEMA = """
    sku STRING,
    product_key STRING,
    category STRING,
    list_price DOUBLE
"""

BRONZE_STORES_SCHEMA = """
    store_id STRING,
    name STRING,
    region STRING
"""

BRONZE_TRANSACTIONS_SCHEMA = """
    transaction_id STRING,
    transaction_line_id STRING,
    transaction_ts TIMESTAMP,
    channel STRING,
    store_id STRING,
    customer_id STRING,
    sku STRING,
    quantity INT,
    unit_price DOUBLE,
    discount DOUBLE,
    currency STRING
"""

# ============================================================================
# Business Rules
# ============================================================================

# Valid channels
VALID_CHANNELS = ["ECOM", "POS"]

# Valid currency
VALID_CURRENCY = ["INR"]

# Minimum and maximum valid prices
MIN_PRICE = 0.0
MAX_PRICE = 10000.0

# Minimum and maximum valid quantity
MIN_QUANTITY = 1
MAX_QUANTITY = 1000

# ============================================================================
# Audit Columns
# ============================================================================

AUDIT_COLUMNS = [
    "_ingestion_date DATE",
    "_batch_id STRING",
    "_source_file STRING",
    "_updated_at TIMESTAMP"
]

# ============================================================================
# Logging Configuration
# ============================================================================

LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR

# ============================================================================
# Helper Functions
# ============================================================================

def get_full_table_name(layer: str, entity: str) -> str:
    """
    Get full table name in format: catalog.schema.table
    
    Args:
        layer: bronze, silver, or gold
        entity: customers, products, stores, transactions, etc.
    
    Returns:
        Full table name
    """
    schema_map = {
        "bronze": BRONZE_SCHEMA,
        "silver": SILVER_SCHEMA,
        "gold": GOLD_SCHEMA
    }
    
    if layer not in schema_map:
        raise ValueError(f"Invalid layer: {layer}. Must be one of {list(schema_map.keys())}")
    
    return f"{CATALOG_NAME}.{schema_map[layer]}.{layer}_{entity}"

def get_volume_path(entity: str = None) -> str:
    """
    Get volume path for a specific entity
    
    Args:
        entity: Optional entity name (customers, products, etc.)
    
    Returns:
        Full volume path
    """
    if entity:
        return f"{BRONZE_SOURCE_PATH}/{entity}"
    return BRONZE_SOURCE_PATH

# ============================================================================
# Validation Configuration
# ============================================================================

# Validation rules for each table
VALIDATION_RULES = {
    "customers": {
        "required_columns": ["customer_id", "name", "signup_date"],
        "not_null_columns": ["customer_id", "name"],
        "unique_columns": ["customer_id"],
        "date_columns": ["signup_date"]
    },
    "products": {
        "required_columns": ["sku", "product_key", "category", "list_price"],
        "not_null_columns": ["sku", "product_key", "list_price"],
        "unique_columns": ["sku"],
        "numeric_columns": ["list_price"],
        "min_values": {"list_price": 0.0}
    },
    "stores": {
        "required_columns": ["store_id", "name", "region"],
        "not_null_columns": ["store_id", "name", "region"],
        "unique_columns": ["store_id"]
    },
    "transactions": {
        "required_columns": ["transaction_id", "sku", "quantity", "unit_price"],
        "not_null_columns": ["transaction_id", "sku", "quantity", "unit_price"],
        "numeric_columns": ["quantity", "unit_price", "discount"],
        "min_values": {"quantity": 1, "unit_price": 0.0},
        "foreign_keys": {
            "customer_id": "customers.customer_id",
            "store_id": "stores.store_id",
            "sku": "products.sku"
        }
    }
}

