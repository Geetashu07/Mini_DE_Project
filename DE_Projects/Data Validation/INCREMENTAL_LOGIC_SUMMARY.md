# Incremental Logic Implementation Summary

## ✅ Implementation Complete

All Silver layer tables now use the **same incremental processing pattern** with `load_audit` table for checkpointing.

## 📊 Tables Updated

### ✅ 1. Customers Table (`silver_customers`)
- **Incremental Logic**: Uses `load_audit` table to track last processed timestamp
- **Merge Key**: `customer_id`
- **Status**: ✅ Already implemented (was the reference)

### ✅ 2. Products Table (`silver_products`)
- **Incremental Logic**: Uses `load_audit` table to track last processed timestamp
- **Merge Key**: `product_key`
- **Status**: ✅ Updated

### ✅ 3. Stores Table (`silver_stores`)
- **Incremental Logic**: Uses `load_audit` table to track last processed timestamp
- **Merge Key**: `store_id`
- **Status**: ✅ Updated

### ✅ 4. Transactions Table (`silver_transactions`)
- **Incremental Logic**: Uses `load_audit` table to track last processed timestamp
- **Merge Key**: `transaction_id` + `transaction_line_id` (composite key)
- **Status**: ✅ Updated

## 🔄 Incremental Logic Pattern

### Step 1: Check if Silver Table Exists

```python
if spark.catalog.tableExists('qc_validation.silver.silver_<table>'):
    # Incremental load
else:
    # Initial load
```

### Step 2: Read Incremental Data from Bronze

```python
bronze_df = spark.sql("""
    SELECT *
    FROM qc_validation.bronze.bronze_<table>
    WHERE _record_ingestion_ts > COALESCE(
        (SELECT MAX(log_ts) FROM qc_validation.default.load_audit 
         WHERE table_name='qc_validation.silver.silver_<table>'),
        TIMESTAMP('1900-01-01 00:00:00')
    )
""")
```

**What it does:**
- Queries `load_audit` table for the last processed timestamp (`log_ts`)
- Selects only records with `_record_ingestion_ts` greater than the last processed timestamp
- Uses `COALESCE` with a default timestamp for first run

### Step 3: Transform Data

- Clean strings
- Standardize case
- Handle nulls
- Remove duplicates
- Add audit columns
- Run QC checks

### Step 4: Write/Merge to Silver

```python
if not spark.catalog.tableExists('qc_validation.silver.silver_<table>'):
    # Initial load: Create table
    silver_df.write.format("delta").saveAsTable('qc_validation.silver.silver_<table>')
else:
    # Incremental load: Merge data
    DeltaTable.forName(spark, 'qc_validation.silver.silver_<table>')
        .alias("final")
        .merge(
            source=silver_df.alias("incr"),
            condition="final.<key> = incr.<key>"
        )
        .whenMatchedUpdateAll()    # Update existing records
        .whenNotMatchedInsertAll() # Insert new records
        .execute()
```

**Merge Keys:**
- **Customers**: `customer_id`
- **Products**: `product_key`
- **Stores**: `store_id`
- **Transactions**: `transaction_id` + `transaction_line_id`

### Step 5: Track Load Audit

```python
if silver_df.count() > 0:
    end_time = time.time()
    end_ts = datetime.now()
    max_df_time = silver_df.agg({"_record_ingestion_ts": "max"}).collect()[0][0]
    load_audit(spark,
            silver_df,
            batch_id,
            'qc_validation.silver.silver_<table>',
            max_df_time,
            start_ts,
            end_ts)
```

**What it tracks:**
- `batch_id`: Unique identifier for the run
- `table_name`: Target table name
- `log_ts`: Maximum `_record_ingestion_ts` processed (checkpoint)
- `start_time`: Processing start timestamp
- `end_time`: Processing end timestamp

## 📋 Load Audit Table Structure

The `load_audit` table (`qc_validation.default.load_audit`) stores:

| Column | Type | Description |
|--------|------|-------------|
| `batch_id` | STRING | Unique batch identifier |
| `table_name` | STRING | Target table name |
| `log_ts` | TIMESTAMP | Maximum `_record_ingestion_ts` processed |
| `start_time` | TIMESTAMP | Processing start time |
| `end_time` | TIMESTAMP | Processing end time |

## 🎯 Benefits

### 1. **Efficiency**
- Only processes new/changed records
- Reduces processing time
- Saves compute resources

### 2. **Idempotency**
- Safe to rerun
- No duplicate processing
- Consistent results

### 3. **Audit Trail**
- Tracks what was processed
- Records processing times
- Enables debugging and monitoring

### 4. **Upsert Logic**
- Updates existing records
- Inserts new records
- Maintains data consistency

### 5. **ACID Compliance**
- Delta Lake merge ensures consistency
- Transactional updates
- No data loss

## 🔍 How It Works

### First Run (Initial Load)
1. Silver table doesn't exist
2. Loads ALL data from Bronze
3. Creates Silver table
4. Tracks load in `load_audit` table

### Subsequent Runs (Incremental Load)
1. Silver table exists
2. Queries `load_audit` for last processed timestamp
3. Loads only NEW records from Bronze (where `_record_ingestion_ts` > last `log_ts`)
4. Merges into Silver table (updates existing, inserts new)
5. Updates `load_audit` with new checkpoint

## 📊 Example Flow

```
Run 1 (Initial Load):
├── Bronze: 1000 records
├── Load: 1000 records
├── Silver: 1000 records
└── load_audit: log_ts = 2025-11-12 10:00:00

Run 2 (Incremental):
├── Bronze: 1000 old + 100 new records
├── Query load_audit: last log_ts = 2025-11-12 10:00:00
├── Load: 100 new records (where _record_ingestion_ts > 2025-11-12 10:00:00)
├── Merge: 100 records into Silver
├── Silver: 1000 records (updated) + 100 new = 1100 records
└── load_audit: log_ts = 2025-11-12 11:00:00

Run 3 (Incremental):
├── Bronze: 1100 old + 50 new records
├── Query load_audit: last log_ts = 2025-11-12 11:00:00
├── Load: 50 new records
├── Merge: 50 records into Silver
├── Silver: 1150 records
└── load_audit: log_ts = 2025-11-12 12:00:00
```

## ✅ Verification Queries

### Check Load Audit Records

```sql
SELECT 
    table_name,
    batch_id,
    log_ts,
    start_time,
    end_time,
    TIMESTAMPDIFF(SECOND, start_time, end_time) as processing_seconds
FROM qc_validation.default.load_audit
ORDER BY log_ts DESC;
```

### Check Last Processed Timestamp

```sql
SELECT 
    table_name,
    MAX(log_ts) as last_processed_ts
FROM qc_validation.default.load_audit
GROUP BY table_name;
```

### Verify Incremental Processing

```sql
-- Check how many records will be processed in next run
SELECT 
    COUNT(*) as new_records
FROM qc_validation.bronze.bronze_customers
WHERE _record_ingestion_ts > COALESCE(
    (SELECT MAX(log_ts) FROM qc_validation.default.load_audit 
     WHERE table_name='qc_validation.silver.silver_customers'),
    TIMESTAMP('1900-01-01 00:00:00')
);
```

## 🎯 Summary

All four Silver tables now have:
- ✅ **Incremental data selection** using `load_audit` table
- ✅ **Delta Lake merge** for upsert operations
- ✅ **Load audit tracking** for checkpointing
- ✅ **Consistent pattern** across all tables

The implementation ensures:
- **Efficient processing** - Only new data is processed
- **Data consistency** - ACID-compliant merges
- **Audit trail** - Complete tracking of all loads
- **Idempotency** - Safe to rerun anytime

## 📝 Notes

- The `load_audit` table is created automatically by the `load_audit()` function
- All tables use the same pattern for consistency
- The checkpoint (`log_ts`) is based on `_record_ingestion_ts` from Bronze layer
- Empty loads (no new data) are not tracked in `load_audit`

