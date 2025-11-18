# Function Lineage Documentation

## 📋 Overview

This document provides a complete function lineage for the Databricks Medallion Architecture project, showing all functions, their locations, dependencies, and call hierarchies.

## 🗂️ Function Organization by Module

### 1. Configuration Module (`config/project_config.py`)

#### Functions:
- **`get_batch_id()`** → Generates unique batch ID
  - **Called by**: All notebooks (Bronze, Silver, Gold, Validation)
  - **Returns**: `str` - Batch ID in format `batch_YYYYMMDD_HHMMSS_UUID`

- **`get_ingestion_date()`** → Gets current ingestion date
  - **Called by**: All notebooks
  - **Returns**: `date` - Current date

- **`get_full_table_name(layer, entity)`** → Constructs full table name
  - **Called by**: Helper functions
  - **Returns**: `str` - Full table name (catalog.schema.table)

- **`get_volume_path(entity)`** → Gets volume path
  - **Called by**: Bronze ingestion
  - **Returns**: `str` - Volume path

---

### 2. Helper Functions Module (`utils/helpers.py`)

#### File Processing Functions:

- **`get_new_files(spark, volume_path, file_pattern, processed_files_table)`**
  - **Location**: `utils/helpers.py:25`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Calls**: `spark.table()`, `dbutils.fs.ls()`
  - **Returns**: `List[str]` - List of new file paths

- **`mark_file_as_processed(spark, processed_files_table, file_path, batch_id, row_count)`**
  - **Location**: `utils/helpers.py:92`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Calls**: `spark.createDataFrame()`, `spark.write.saveAsTable()`
  - **Returns**: `None` (writes to Delta table)

#### Audit Column Functions:

- **`add_audit_columns_bronze(df, batch_id, _record_ingestion_ts, source_file)`**
  - **Location**: `utils/helpers.py:131`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Calls**: PySpark functions (`to_date()`, `current_timestamp()`, `lit()`, `to_timestamp()`)
  - **Returns**: `DataFrame` - DataFrame with audit columns

- **`add_audit_columns(df, batch_id, source_file)`**
  - **Location**: `utils/helpers.py:157`
  - **Called by**: `transform_customers()`, `transform_products()`, `transform_stores()`, `transform_transactions()` (Silver notebook)
  - **Calls**: PySpark functions (`to_date()`, `current_timestamp()`, `lit()`)
  - **Returns**: `DataFrame` - DataFrame with audit columns

- **`get_latest_batch_id(spark, table_name)`**
  - **Location**: `utils/helpers.py:182`
  - **Called by**: Helper functions (currently unused in notebooks)
  - **Returns**: `Optional[str]` - Latest batch ID or None

#### Data Cleaning Functions:

- **`clean_string_columns(df, columns)`**
  - **Location**: `utils/helpers.py:212`
  - **Called by**: 
    - `transform_customers()` (Silver)
    - `transform_products()` (Silver)
    - `transform_stores()` (Silver)
    - `transform_transactions()` (Silver)
  - **Calls**: PySpark `trim()`, `col()`
  - **Returns**: `DataFrame` - DataFrame with trimmed strings

- **`standardize_string_case(df, columns, case)`**
  - **Location**: `utils/helpers.py:236`
  - **Called by**: 
    - `transform_customers()` (Silver)
    - `transform_products()` (Silver)
    - `transform_stores()` (Silver)
    - `transform_transactions()` (Silver)
  - **Calls**: PySpark `upper()`, `lower()`, `col()`
  - **Returns**: `DataFrame` - DataFrame with standardized case

- **`handle_nulls(df, column_defaults)`**
  - **Location**: `utils/helpers.py:268`
  - **Called by**: 
    - `transform_customers()` (Silver)
    - `transform_products()` (Silver)
    - `transform_stores()` (Silver)
    - `transform_transactions()` (Silver)
  - **Calls**: PySpark `coalesce()`, `lit()`, `col()`
  - **Returns**: `DataFrame` - DataFrame with nulls handled

- **`remove_duplicates(df, key_columns, keep)`**
  - **Location**: `utils/helpers.py:292`
  - **Called by**: 
    - `transform_customers()` (Silver)
    - `transform_products()` (Silver)
    - `transform_stores()` (Silver)
    - `transform_transactions()` (Silver)
  - **Calls**: PySpark `Window()`, `row_number()`, `current_timestamp()`, `col()`
  - **Returns**: `DataFrame` - DataFrame with duplicates removed

#### Data Type Conversion Functions:

- **`convert_data_types(df, type_mapping)`**
  - **Location**: `utils/helpers.py:341`
  - **Called by**: Helper functions (currently unused in notebooks)
  - **Returns**: `DataFrame` - DataFrame with converted types

#### Table Management Functions:

- **`create_table_if_not_exists(spark, table_name, schema, partition_columns)`**
  - **Location**: `utils/helpers.py:369`
  - **Called by**: Helper functions (currently unused in notebooks)
  - **Calls**: `spark.sql()`
  - **Returns**: `None`

- **`merge_data(spark, target_table, source_df, key_columns, update_columns)`**
  - **Location**: `utils/helpers.py:406`
  - **Called by**: Helper functions (currently unused in notebooks)
  - **Calls**: `spark.sql()`, `source_df.createOrReplaceTempView()`
  - **Returns**: `None`


#### Logging Functions:


- **`log_processing_stats(spark, table_name, batch_id, row_count, processing_time)`**
  - **Location**: `utils/helpers.py:460`
  - **Called by**: Helper functions (currently unused in notebooks)
  - **Returns**: `None` (prints statistics)

- **`load_audit(spark, df, batch_id, table_name, max_df_time, start_time, end_time)`**
  - **Location**: `utils/helpers.py:493`
  - **Called by**: 
    - `ingest_csv_to_bronze()` (Bronze notebook)
    - `transform_customers()` (Silver notebook)
    - `transform_products()` (Silver notebook)
    - `transform_stores()` (Silver notebook)
    - `transform_transactions()` (Silver notebook)
  - **Calls**: `spark.createDataFrame()`, `spark.catalog.tableExists()`, `spark.write.saveAsTable()`
  - **Returns**: `None` (writes to Delta table)

---

### 3. QC Framework Module (`utils/qc_framework.py`)

#### QC Result Class:

- **`QCResult`** (Class)
  - **Location**: `utils/qc_framework.py:24`
  - **Methods**:
    - `__init__()` - Initialize QC result
    - `to_dict()` - Convert to dictionary
    - `__repr__()` - String representation
  - **Used by**: All QC check functions
  - **Returns**: `QCResult` object

#### Basic QC Check Functions:

- **`check_row_count(df, table_name, min_rows, expected_rows)`**
  - **Location**: `utils/qc_framework.py:69`
  - **Called by**: 
    - `run_qc_checks()` (QC Framework)
    - `ingest_csv_to_bronze()` (Bronze notebook)
    - `validate_bronze_layer()` (Validation notebook)
    - `validate_silver_layer()` (Validation notebook)
    - `validate_gold_layer()` (Validation notebook)
  - **Calls**: `df.count()`, `QCResult()`
  - **Returns**: `QCResult`

- **`check_nulls(df, table_name, columns, max_null_percentage)`**
  - **Location**: `utils/qc_framework.py:115`
  - **Called by**: `run_qc_checks()` (QC Framework)
  - **Calls**: `df.count()`, `df.filter()`, `col().isNull()`, `QCResult()`
  - **Returns**: `List[QCResult]`

- **`check_duplicates(df, table_name, key_columns, max_duplicate_percentage)`**
  - **Location**: `utils/qc_framework.py:173`
  - **Called by**: `run_qc_checks()` (QC Framework)
  - **Calls**: `df.count()`, `df.groupBy()`, `spark_sum()`, `QCResult()`
  - **Returns**: `QCResult`

- **`check_schema(df, table_name, expected_schema)`**
  - **Location**: `utils/qc_framework.py:232`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Calls**: `df.schema`, `df.columns`, `QCResult()`
  - **Returns**: `QCResult`

#### Referential Integrity Functions:

- **`check_referential_integrity(df, table_name, foreign_key_column, reference_table, reference_key_column)`**
  - **Location**: `utils/qc_framework.py:299`
  - **Called by**: 
    - `transform_transactions()` (Silver notebook)
    - `validate_silver_layer()` (Validation notebook)
  - **Calls**: `df.count()`, `df.join()`, `df.filter()`, `col().isNotNull()`, `QCResult()`
  - **Returns**: `QCResult`

#### Business Rule Check Functions:

- **`check_value_range(df, table_name, column, min_value, max_value)`**
  - **Location**: `utils/qc_framework.py:357`
  - **Called by**: `run_qc_checks()` (QC Framework)
  - **Calls**: `df.count()`, `df.filter()`, `col()`, `when()`, `lit()`, `QCResult()`
  - **Returns**: `QCResult`

- **`check_valid_values(df, table_name, column, valid_values)`**
  - **Location**: `utils/qc_framework.py:432`
  - **Called by**: `run_qc_checks()` (QC Framework)
  - **Calls**: `df.count()`, `df.filter()`, `col().isNotNull()`, `col().isin()`, `QCResult()`
  - **Returns**: `QCResult`

#### Comprehensive QC Functions:

- **`run_qc_checks(df, table_name, checks)`**
  - **Location**: `utils/qc_framework.py:490`
  - **Called by**: 
    - `ingest_csv_to_bronze()` (Bronze notebook)
    - `transform_customers()` (Silver notebook)
    - `transform_products()` (Silver notebook)
    - `transform_stores()` (Silver notebook)
    - `transform_transactions()` (Silver notebook)
    - `create_customer_summary()` (Gold notebook)
    - `create_product_sales()` (Gold notebook)
    - `create_store_performance()` (Gold notebook)
    - `create_daily_sales()` (Gold notebook)
    - `validate_bronze_layer()` (Validation notebook)
    - `validate_silver_layer()` (Validation notebook)
    - `validate_gold_layer()` (Validation notebook)
  - **Calls**: 
    - `check_row_count()`
    - `check_nulls()`
    - `check_duplicates()`
    - `check_value_range()`
    - `check_valid_values()`
  - **Returns**: `List[QCResult]`

- **`save_qc_results(spark, results, results_table, batch_id)`**
  - **Location**: `utils/qc_framework.py:543`
  - **Called by**: 
    - `validate_bronze_layer()` (Validation notebook)
    - `validate_silver_layer()` (Validation notebook)
    - `validate_gold_layer()` (Validation notebook)
  - **Calls**: `result.to_dict()`, `spark.createDataFrame()`, `spark.write.saveAsTable()`
  - **Returns**: `None` (writes to Delta table)

---

### 4. Bronze Ingestion Notebook (`notebooks/01_bronze_ingestion.py`)

#### Schema Definition Functions:

- **`get_customers_schema()`**
  - **Location**: `notebooks/01_bronze_ingestion.py:139`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Returns**: `StructType` - Schema for customers

- **`get_products_schema()`**
  - **Location**: `notebooks/01_bronze_ingestion.py:152`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Returns**: `StructType` - Schema for products

- **`get_stores_schema()`**
  - **Location**: `notebooks/01_bronze_ingestion.py:165`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Returns**: `StructType` - Schema for stores

- **`get_transactions_schema()`**
  - **Location**: `notebooks/01_bronze_ingestion.py:178`
  - **Called by**: `ingest_csv_to_bronze()` (Bronze notebook)
  - **Returns**: `StructType` - Schema for transactions

#### Main Ingestion Function:

- **`ingest_csv_to_bronze(entity_name, file_pattern, schema, target_table, header)`**
  - **Location**: `notebooks/01_bronze_ingestion.py:206`
  - **Called by**: 
    - Direct calls for each entity (customers, products, stores, transactions)
  - **Calls**:
    - `dbutils.fs.ls()` (Databricks built-in)
    - `get_new_files()` (helpers.py)
    - `spark.read.csv()` (PySpark)
    - `add_audit_columns_bronze()` (helpers.py)
    - `df.count()` (PySpark)
    - `check_schema()` (qc_framework.py)
    - `mark_file_as_processed()` (helpers.py)
    - `df.union()` (PySpark)
    - `run_qc_checks()` (qc_framework.py)
    - `load_audit()` (helpers.py)
  - **Returns**: `None` (writes to Delta table)

---

### 5. Silver Transformation Notebook (`notebooks/02_silver_transformation.py`)

#### Transformation Functions:

- **`transform_customers()`**
  - **Location**: `notebooks/02_silver_transformation.py:92`
  - **Called by**: Direct call in notebook
  - **Calls**:
    - `spark.sql()` (PySpark)
    - `spark.table()` (PySpark)
    - `clean_string_columns()` (helpers.py)
    - `standardize_string_case()` (helpers.py)
    - `handle_nulls()` (helpers.py)
    - `remove_duplicates()` (helpers.py)
    - `add_audit_columns()` (helpers.py)
    - `run_qc_checks()` (qc_framework.py)
    - `DeltaTable.forName().merge()` (Delta Lake)
    - `load_audit()` (helpers.py)
  - **Returns**: `List[QCResult]`

- **`transform_products()`**
  - **Location**: `notebooks/02_silver_transformation.py:215`
  - **Called by**: Direct call in notebook
  - **Calls**: Same as `transform_customers()`
  - **Returns**: `List[QCResult]`

- **`transform_stores()`**
  - **Location**: `notebooks/02_silver_transformation.py:336`
  - **Called by**: Direct call in notebook
  - **Calls**: Same as `transform_customers()`
  - **Returns**: `List[QCResult]`

- **`transform_transactions()`**
  - **Location**: `notebooks/02_silver_transformation.py:453`
  - **Called by**: Direct call in notebook
  - **Calls**: 
    - Same as `transform_customers()` PLUS
    - `check_referential_integrity()` (qc_framework.py) - for customer_id, store_id, sku
  - **Returns**: `List[QCResult]`

---

### 6. Gold Aggregation Notebook (`notebooks/03_gold_aggregation.py`)

#### Aggregation Functions:

- **`create_customer_summary()`**
  - **Location**: `notebooks/03_gold_aggregation.py:85`
  - **Called by**: Direct call in notebook
  - **Calls**:
    - `spark.table()` (PySpark)
    - `df.groupBy().agg()` (PySpark aggregations)
    - `df.join()` (PySpark)
    - `coalesce()`, `lit()`, `round()` (PySpark)
    - `run_qc_checks()` (qc_framework.py)
    - `add_audit_columns()` (helpers.py)
    - `spark.write.saveAsTable()` (PySpark)
  - **Returns**: `List[QCResult]`

- **`create_product_sales()`**
  - **Location**: `notebooks/03_gold_aggregation.py:180`
  - **Called by**: Direct call in notebook
  - **Calls**: Same as `create_customer_summary()`
  - **Returns**: `List[QCResult]`

- **`create_store_performance()`**
  - **Location**: `notebooks/03_gold_aggregation.py:281`
  - **Called by**: Direct call in notebook
  - **Calls**: Same as `create_customer_summary()`
  - **Returns**: `List[QCResult]`

- **`create_daily_sales()`**
  - **Location**: `notebooks/03_gold_aggregation.py:378`
  - **Called by**: Direct call in notebook
  - **Calls**: Same as `create_customer_summary()`
  - **Returns**: `List[QCResult]`

---

### 7. Validation Notebook (`validation/04_validation_qc.py`)

#### Validation Functions:

- **`validate_bronze_layer()`**
  - **Location**: `validation/04_validation_qc.py:108`
  - **Called by**: Direct call in notebook
  - **Calls**:
    - `spark.table()` (PySpark)
    - `run_qc_checks()` (qc_framework.py)
    - `save_qc_results()` (qc_framework.py)
  - **Returns**: `List[QCResult]`

- **`validate_silver_layer()`**
  - **Location**: `validation/04_validation_qc.py:181`
  - **Called by**: Direct call in notebook
  - **Calls**:
    - `spark.table()` (PySpark)
    - `run_qc_checks()` (qc_framework.py)
    - `check_referential_integrity()` (qc_framework.py)
    - `save_qc_results()` (qc_framework.py)
  - **Returns**: `List[QCResult]`

- **`validate_gold_layer()`**
  - **Location**: `validation/04_validation_qc.py:314`
  - **Called by**: Direct call in notebook
  - **Calls**:
    - `spark.table()` (PySpark)
    - `run_qc_checks()` (qc_framework.py)
    - `QCResult()` (qc_framework.py) - for business rules
    - `save_qc_results()` (qc_framework.py)
  - **Returns**: `List[QCResult]`

---

## 🔄 Function Call Hierarchy

### Bronze Layer Flow

```
01_bronze_ingestion.py
    │
    ├── ingest_csv_to_bronze()
    │   │
    │   ├── get_batch_id() [config/project_config.py]
    │   ├── get_ingestion_date() [config/project_config.py]
    │   │
    │   ├── dbutils.fs.ls() [Databricks built-in]
    │   ├── get_new_files() [utils/helpers.py]
    │   │   └── spark.table() [PySpark]
    │   │   └── dbutils.fs.ls() [Databricks built-in]
    │   │
    │   ├── spark.read.csv() [PySpark]
    │   ├── add_audit_columns_bronze() [utils/helpers.py]
    │   │   └── to_date(), current_timestamp(), lit(), to_timestamp() [PySpark]
    │   │
    │   ├── check_schema() [utils/qc_framework.py]
    │   │   └── QCResult() [utils/qc_framework.py]
    │   │
    │   ├── mark_file_as_processed() [utils/helpers.py]
    │   │   └── spark.createDataFrame() [PySpark]
    │   │   └── spark.write.saveAsTable() [PySpark]
    │   │
    │   ├── run_qc_checks() [utils/qc_framework.py]
    │   │   └── check_row_count() [utils/qc_framework.py]
    │   │       └── QCResult() [utils/qc_framework.py]
    │   │
    │   └── load_audit() [utils/helpers.py]
    │       └── spark.createDataFrame() [PySpark]
    │       └── spark.write.saveAsTable() [PySpark]
    │
    ├── get_customers_schema()
    ├── get_products_schema()
    ├── get_stores_schema()
    └── get_transactions_schema()
```

### Silver Layer Flow

```
02_silver_transformation.py
    │
    ├── transform_customers()
    │   │
    │   ├── get_batch_id() [config/project_config.py]
    │   ├── get_ingestion_date() [config/project_config.py]
    │   │
    │   ├── spark.sql() [PySpark] - Incremental query
    │   ├── spark.table() [PySpark]
    │   │
    │   ├── clean_string_columns() [utils/helpers.py]
    │   │   └── trim(), col() [PySpark]
    │   │
    │   ├── standardize_string_case() [utils/helpers.py]
    │   │   └── upper(), lower(), col() [PySpark]
    │   │
    │   ├── handle_nulls() [utils/helpers.py]
    │   │   └── coalesce(), lit(), col() [PySpark]
    │   │
    │   ├── remove_duplicates() [utils/helpers.py]
    │   │   └── Window(), row_number(), current_timestamp() [PySpark]
    │   │
    │   ├── add_audit_columns() [utils/helpers.py]
    │   │   └── to_date(), current_timestamp(), lit() [PySpark]
    │   │
    │   ├── run_qc_checks() [utils/qc_framework.py]
    │   │   ├── check_row_count() [utils/qc_framework.py]
    │   │   ├── check_nulls() [utils/qc_framework.py]
    │   │   └── check_duplicates() [utils/qc_framework.py]
    │   │
    │   ├── DeltaTable.forName().merge() [Delta Lake]
    │   │
    │   └── load_audit() [utils/helpers.py]
    │
    ├── transform_products() [Same pattern as transform_customers]
    ├── transform_stores() [Same pattern as transform_customers]
    └── transform_transactions() [Same pattern + check_referential_integrity]
```

### Gold Layer Flow

```
03_gold_aggregation.py
    │
    ├── create_customer_summary()
    │   │
    │   ├── get_batch_id() [config/project_config.py]
    │   ├── get_ingestion_date() [config/project_config.py]
    │   │
    │   ├── spark.table() [PySpark]
    │   ├── df.groupBy().agg() [PySpark aggregations]
    │   ├── df.join() [PySpark]
    │   ├── coalesce(), lit(), round() [PySpark]
    │   │
    │   ├── run_qc_checks() [utils/qc_framework.py]
    │   │   └── check_row_count() [utils/qc_framework.py]
    │   │   └── check_value_range() [utils/qc_framework.py]
    │   │
    │   └── spark.write.saveAsTable() [PySpark]
    │
    ├── create_product_sales() [Same pattern]
    ├── create_store_performance() [Same pattern]
    └── create_daily_sales() [Same pattern]
```

### Validation Layer Flow

```
04_validation_qc.py
    │
    ├── validate_bronze_layer()
    │   │
    │   ├── get_batch_id() [config/project_config.py]
    │   ├── get_ingestion_date() [config/project_config.py]
    │   │
    │   ├── spark.table() [PySpark]
    │   ├── run_qc_checks() [utils/qc_framework.py]
    │   │   ├── check_row_count() [utils/qc_framework.py]
    │   │   └── check_nulls() [utils/qc_framework.py]
    │   │
    │   └── save_qc_results() [utils/qc_framework.py]
    │       └── result.to_dict() [QCResult method]
    │       └── spark.createDataFrame() [PySpark]
    │
    ├── validate_silver_layer()
    │   │
    │   ├── spark.table() [PySpark]
    │   ├── run_qc_checks() [utils/qc_framework.py]
    │   ├── check_referential_integrity() [utils/qc_framework.py]
    │   └── save_qc_results() [utils/qc_framework.py]
    │
    └── validate_gold_layer()
        │
        ├── spark.table() [PySpark]
        ├── run_qc_checks() [utils/qc_framework.py]
        ├── QCResult() [utils/qc_framework.py] - Business rules
        └── save_qc_results() [utils/qc_framework.py]
```

---

## 📊 Function Dependency Matrix

### Functions by Module

| Module | Function Count | Functions |
|--------|---------------|-----------|
| `config/project_config.py` | 4 | `get_batch_id()`, `get_ingestion_date()`, `get_full_table_name()`, `get_volume_path()` |
| `utils/helpers.py` | 12 | `get_new_files()`, `mark_file_as_processed()`, `add_audit_columns_bronze()`, `add_audit_columns()`, `get_latest_batch_id()`, `clean_string_columns()`, `standardize_string_case()`, `handle_nulls()`, `remove_duplicates()`, `convert_data_types()`, `create_table_if_not_exists()`, `merge_data()`, `log_processing_stats()`, `load_audit()` |
| `utils/qc_framework.py` | 10 | `QCResult` (class), `check_row_count()`, `check_nulls()`, `check_duplicates()`, `check_schema()`, `check_referential_integrity()`, `check_value_range()`, `check_valid_values()`, `run_qc_checks()`, `save_qc_results()` |
| `notebooks/01_bronze_ingestion.py` | 5 | `get_customers_schema()`, `get_products_schema()`, `get_stores_schema()`, `get_transactions_schema()`, `ingest_csv_to_bronze()` |
| `notebooks/02_silver_transformation.py` | 4 | `transform_customers()`, `transform_products()`, `transform_stores()`, `transform_transactions()` |
| `notebooks/03_gold_aggregation.py` | 4 | `create_customer_summary()`, `create_product_sales()`, `create_store_performance()`, `create_daily_sales()` |
| `validation/04_validation_qc.py` | 3 | `validate_bronze_layer()`, `validate_silver_layer()`, `validate_gold_layer()` |

**Total Functions**: 42 functions + 1 class

---

## 🔗 Cross-Module Dependencies

### Most Used Functions

1. **`run_qc_checks()`** - Used in 12 places
   - Bronze: 1x
   - Silver: 4x (customers, products, stores, transactions)
   - Gold: 4x (customer_summary, product_sales, store_performance, daily_sales)
   - Validation: 3x (bronze, silver, gold layers)

2. **`load_audit()`** - Used in 5 places
   - Bronze: 1x
   - Silver: 4x (all transformation functions)

3. **`add_audit_columns()`** - Used in 4 places
   - Silver: 4x (all transformation functions)

4. **`clean_string_columns()`** - Used in 4 places
   - Silver: 4x (all transformation functions)

5. **`standardize_string_case()`** - Used in 4 places
   - Silver: 4x (all transformation functions)

6. **`handle_nulls()`** - Used in 4 places
   - Silver: 4x (all transformation functions)

7. **`remove_duplicates()`** - Used in 4 places
   - Silver: 4x (all transformation functions)

---

## 📈 Function Call Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE INGESTION                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │  ingest_csv_to_bronze()              │
        └─────────────────────────────────────┘
                  │
        ┌─────────┴─────────┬──────────────┬──────────────┐
        ▼                   ▼              ▼              ▼
  get_new_files()   add_audit_columns_  check_schema()  mark_file_as_
  [helpers.py]      bronze()           [qc_framework]  processed()
                    [helpers.py]                       [helpers.py]
        │                   │              │              │
        └───────────────────┴──────────────┴──────────────┘
                              │
                              ▼
                    run_qc_checks()
                    [qc_framework.py]
                              │
                              ▼
                    load_audit()
                    [helpers.py]

┌─────────────────────────────────────────────────────────────────┐
│                    SILVER TRANSFORMATION                         │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
        ▼                                           ▼
transform_customers()                    transform_transactions()
transform_products()                     (includes referential
transform_stores()                        integrity checks)
        │                                           │
        └───────────────┬───────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
clean_string_columns()          check_referential_integrity()
standardize_string_case()       [qc_framework.py]
handle_nulls()
remove_duplicates()
add_audit_columns()
[all from helpers.py]
        │                               │
        └───────────────┬───────────────┘
                        │
                        ▼
                run_qc_checks()
                [qc_framework.py]
                        │
                        ▼
                DeltaTable.merge()
                [Delta Lake]
                        │
                        ▼
                load_audit()
                [helpers.py]

┌─────────────────────────────────────────────────────────────────┐
│                    GOLD AGGREGATION                              │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
        ▼                                           ▼
create_customer_summary()              create_daily_sales()
create_product_sales()                  create_store_performance()
        │                                           │
        └───────────────┬───────────────────────────┘
                        │
                        ▼
                PySpark Aggregations
                (groupBy, agg, join)
                        │
                        ▼
                run_qc_checks()
                [qc_framework.py]
                        │
                        ▼
                spark.write.saveAsTable()
                [PySpark]

┌─────────────────────────────────────────────────────────────────┐
│                    VALIDATION QC                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
        ▼                                           ▼
validate_bronze_layer()                validate_gold_layer()
validate_silver_layer()                (includes business rules)
        │                                           │
        └───────────────┬───────────────────────────┘
                        │
                        ▼
                run_qc_checks()
                [qc_framework.py]
                        │
                        ▼
                save_qc_results()
                [qc_framework.py]
                        │
                        ▼
                validation_results table
                [Delta Lake]
```

---

## 🔍 Function Import Map

### Bronze Notebook Imports

```python
from project_config import:
    - CATALOG_NAME, BRONZE_SCHEMA
    - BRONZE_*_TABLE (all table names)
    - VOLUME_PATH, BRONZE_SOURCE_PATH
    - *_FILE_PATTERN (all file patterns)
    - PROCESSED_FILES_TABLE
    - get_batch_id(), get_ingestion_date()
    - BRONZE_*_SCHEMA (all schemas)

from helpers import:
    - get_new_files()
    - mark_file_as_processed()
    - add_audit_columns_bronze()

from qc_framework import:
    - check_schema()
    - run_qc_checks()
```

### Silver Notebook Imports

```python
from project_config import:
    - CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA
    - BRONZE_*_TABLE, SILVER_*_TABLE
    - get_batch_id(), get_ingestion_date()
    - VALIDATION_RULES, VALID_CHANNELS, VALID_CURRENCY
    - MIN_PRICE, MAX_PRICE, MIN_QUANTITY, MAX_QUANTITY

from helpers import:
    - clean_string_columns()
    - standardize_string_case()
    - handle_nulls()
    - remove_duplicates()
    - add_audit_columns()
    - load_audit()

from qc_framework import:
    - check_row_count()
    - check_nulls()
    - check_duplicates()
    - check_referential_integrity()
    - check_value_range()
    - check_valid_values()
    - run_qc_checks()
```

### Gold Notebook Imports

```python
from project_config import:
    - CATALOG_NAME, SILVER_SCHEMA, GOLD_SCHEMA
    - SILVER_*_TABLE, GOLD_*_TABLE
    - get_batch_id(), get_ingestion_date()

from qc_framework import:
    - check_row_count()
    - check_value_range()
    - run_qc_checks()
    - save_qc_results()

from helpers import:
    - add_audit_columns()
```

### Validation Notebook Imports

```python
from project_config import:
    - CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA
    - BRONZE_*_TABLE, SILVER_*_TABLE, GOLD_*_TABLE
    - VALIDATION_RESULTS_TABLE
    - get_batch_id(), get_ingestion_date()

from qc_framework import:
    - check_row_count()
    - check_nulls()
    - check_duplicates()
    - check_referential_integrity()
    - check_value_range()
    - check_valid_values()
    - run_qc_checks()
    - save_qc_results()
    - QCResult (class)
```

---

## 📝 Function Summary Table

| Function Name | Module | Called By | Calls | Purpose |
|--------------|--------|-----------|-------|---------|
| `get_batch_id()` | config | All notebooks | `datetime.now()`, `uuid.uuid4()` | Generate unique batch ID |
| `get_ingestion_date()` | config | All notebooks | `datetime.now()` | Get current date |
| `get_new_files()` | helpers | Bronze notebook | `spark.table()`, `dbutils.fs.ls()` | Find new files to process |
| `mark_file_as_processed()` | helpers | Bronze notebook | `spark.createDataFrame()`, `spark.write.saveAsTable()` | Track processed files |
| `add_audit_columns_bronze()` | helpers | Bronze notebook | PySpark functions | Add audit columns to Bronze |
| `add_audit_columns()` | helpers | Silver notebook | PySpark functions | Add audit columns to Silver |
| `clean_string_columns()` | helpers | Silver notebook | `trim()`, `col()` | Trim whitespace |
| `standardize_string_case()` | helpers | Silver notebook | `upper()`, `lower()`, `col()` | Standardize case |
| `handle_nulls()` | helpers | Silver notebook | `coalesce()`, `lit()`, `col()` | Handle null values |
| `remove_duplicates()` | helpers | Silver notebook | `Window()`, `row_number()` | Remove duplicates |
| `load_audit()` | helpers | Bronze, Silver | `spark.createDataFrame()`, `spark.write.saveAsTable()` | Track load metadata |
| `check_row_count()` | qc_framework | Multiple | `df.count()`, `QCResult()` | Validate row count |
| `check_nulls()` | qc_framework | Multiple | `df.filter()`, `col().isNull()`, `QCResult()` | Check for nulls |
| `check_duplicates()` | qc_framework | Multiple | `df.groupBy()`, `spark_sum()`, `QCResult()` | Check duplicates |
| `check_schema()` | qc_framework | Bronze notebook | `df.schema`, `df.columns`, `QCResult()` | Validate schema |
| `check_referential_integrity()` | qc_framework | Silver, Validation | `df.join()`, `df.filter()`, `QCResult()` | Check foreign keys |
| `check_value_range()` | qc_framework | Multiple | `df.filter()`, `col()`, `QCResult()` | Validate value ranges |
| `check_valid_values()` | qc_framework | Multiple | `df.filter()`, `col().isin()`, `QCResult()` | Validate allowed values |
| `run_qc_checks()` | qc_framework | All notebooks | All check functions | Run multiple QC checks |
| `save_qc_results()` | qc_framework | Validation notebook | `result.to_dict()`, `spark.createDataFrame()` | Save QC results |
| `ingest_csv_to_bronze()` | Bronze notebook | Direct calls | Multiple helper functions | Ingest CSV to Bronze |
| `transform_customers()` | Silver notebook | Direct call | Multiple helper functions | Transform customers |
| `transform_products()` | Silver notebook | Direct call | Multiple helper functions | Transform products |
| `transform_stores()` | Silver notebook | Direct call | Multiple helper functions | Transform stores |
| `transform_transactions()` | Silver notebook | Direct call | Multiple helper functions | Transform transactions |
| `create_customer_summary()` | Gold notebook | Direct call | PySpark aggregations, QC functions | Create customer summary |
| `create_product_sales()` | Gold notebook | Direct call | PySpark aggregations, QC functions | Create product sales |
| `create_store_performance()` | Gold notebook | Direct call | PySpark aggregations, QC functions | Create store performance |
| `create_daily_sales()` | Gold notebook | Direct call | PySpark aggregations, QC functions | Create daily sales |
| `validate_bronze_layer()` | Validation notebook | Direct call | QC functions | Validate Bronze layer |
| `validate_silver_layer()` | Validation notebook | Direct call | QC functions | Validate Silver layer |
| `validate_gold_layer()` | Validation notebook | Direct call | QC functions | Validate Gold layer |

---

## 🎯 Key Function Relationships

### Core Utility Functions (Most Reusable)

1. **`run_qc_checks()`** - Central QC orchestrator
2. **`load_audit()`** - Incremental processing checkpoint
3. **`add_audit_columns()`** - Audit trail management
4. **`clean_string_columns()`** - Data cleaning
5. **`remove_duplicates()`** - Data deduplication

### Layer-Specific Functions

- **Bronze**: `ingest_csv_to_bronze()`, `get_*_schema()`
- **Silver**: `transform_*()` functions
- **Gold**: `create_*()` aggregation functions
- **Validation**: `validate_*_layer()` functions

### Configuration Functions

- **`get_batch_id()`** - Used by all notebooks
- **`get_ingestion_date()`** - Used by all notebooks

---

## 📚 Quick Reference

### Find a Function

- **Config functions**: `config/project_config.py`
- **Helper functions**: `utils/helpers.py`
- **QC functions**: `utils/qc_framework.py`
- **Bronze functions**: `notebooks/01_bronze_ingestion.py`
- **Silver functions**: `notebooks/02_silver_transformation.py`
- **Gold functions**: `notebooks/03_gold_aggregation.py`
- **Validation functions**: `validation/04_validation_qc.py`

### Function Categories

- **Data Ingestion**: `ingest_csv_to_bronze()`, `get_new_files()`, `mark_file_as_processed()`
- **Data Transformation**: `transform_*()`, `clean_string_columns()`, `standardize_string_case()`, `handle_nulls()`, `remove_duplicates()`
- **Data Aggregation**: `create_*()` functions in Gold notebook
- **Data Quality**: All `check_*()` functions, `run_qc_checks()`, `save_qc_results()`
- **Audit & Tracking**: `add_audit_columns()`, `add_audit_columns_bronze()`, `load_audit()`
- **Validation**: `validate_*_layer()` functions

---

This lineage document provides a complete map of all functions, their locations, dependencies, and relationships in the project.

