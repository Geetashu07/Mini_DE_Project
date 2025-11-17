"""
Helper Functions for Databricks Medallion Architecture

This module provides utility functions for common operations:
- File processing
- Incremental loading
- Audit column management
- Data transformation helpers
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, trim, upper, lower,
    when, isnull, coalesce, regexp_replace, date_format, row_number,to_timestamp
)
from pyspark.sql.types import StructType
from typing import List, Optional, Dict
from datetime import datetime
import os

# ============================================================================
# File Processing Helpers
# ============================================================================

def get_new_files(spark: SparkSession,
                 volume_path: str,
                 file_pattern: str,
                 processed_files_table: str) -> List[str]:
    """
    Get list of new files that haven't been processed yet
    
    Args:
        spark: SparkSession
        volume_path: Path to the volume directory
        file_pattern: File pattern to match (e.g., "*.csv")
        processed_files_table: Table storing processed file names
    
    Returns:
        List of file paths that are new
    """
    print('Inside get_new_files -->')
    try:
        # Get all files matching the pattern using dbutils
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except:
            # If dbutils is not available, use alternative method
            dbutils = None
        
        all_files = []
        if dbutils:
            # Use dbutils to list files
            try:
                files = dbutils.fs.ls(volume_path)
                for file_info in files:
                    file_path = file_info.path
                    file_name = file_info.name
                    if not file_info.isDir() and file_pattern.replace("*", "") in file_name:
                        all_files.append(file_path)
            except Exception as e:
                print(f"Error listing files with dbutils: {str(e)}")
        else:
            # Alternative: Use spark to read directory (may not work for volumes)
            try:
                files_df = spark.sql(f"LIST '{volume_path}'")
                for row in files_df.collect():
                    file_path = row.path
                    if not row.isDir and file_pattern.replace("*", "") in file_path:
                        all_files.append(file_path)
            except Exception as e:
                print(f"Error listing files with LIST: {str(e)}")
        
        # Get processed files
        processed_files = []
        try:
            processed_df = spark.table(processed_files_table)
            processed_files = [row.file_path for row in processed_df.select("file_path").distinct().collect()]
        except Exception:
            # Table doesn't exist yet, so no files have been processed
            pass
        
        # Return new files
        new_files = [f for f in all_files if f not in processed_files]
        return new_files
    
    except Exception as e:
        print(f"Error getting new files: {str(e)}")
        return []


def mark_file_as_processed(spark: SparkSession,
                          processed_files_table: str,
                          file_path: str,
                          batch_id: str,
                          row_count: int):
    """
    Mark a file as processed in the processed files table
    
    Args:
        spark: SparkSession
        processed_files_table: Table storing processed file names
        file_path: Path to the processed file
        batch_id: Batch ID for the current run
        row_count: Number of rows processed
    """
    from pyspark.sql import Row
    
    processed_data = [Row(
        file_path=file_path,
        batch_id=batch_id,
        row_count=row_count,
        processed_at=datetime.now()
    )]
    
    processed_df = spark.createDataFrame(processed_data)
    processed_df.printSchema()
    processed_df = processed_df.withColumn("row_count", col("row_count").cast("int"))
    
    # Append to processed files table
    processed_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(processed_files_table)


# ============================================================================
# Audit Column Management
# ============================================================================

def add_audit_columns_bronze(df: DataFrame,
                     batch_id: str,
                     _record_ingestion_ts ,
                     source_file: str = None) -> DataFrame:
    """
    Add audit columns to DataFrame
    
    Args:
        df: DataFrame to add audit columns to
        batch_id: Batch ID for the current run
        source_file: Source file name (optional)
    
    Returns:
        DataFrame with audit columns added
    """
    df_with_audit = df \
        .withColumn("_ingestion_date", to_date(current_timestamp())) \
        .withColumn("_batch_id", lit(batch_id)) \
        .withColumn("_updated_at", current_timestamp())\
        .withColumn("_record_ingestion_ts", to_timestamp(lit(_record_ingestion_ts)))
    
    if source_file:
        df_with_audit = df_with_audit.withColumn("_source_file", lit(source_file))
    
    return df_with_audit

def add_audit_columns(df: DataFrame,
                     batch_id: str,
                     source_file: str = None) -> DataFrame:
    """
    Add audit columns to DataFrame
    
    Args:
        df: DataFrame to add audit columns to
        batch_id: Batch ID for the current run
        source_file: Source file name (optional)
    
    Returns:
        DataFrame with audit columns added
    """
    df_with_audit = df \
        .withColumn("_ingestion_date", to_date(current_timestamp())) \
        .withColumn("_batch_id", lit(batch_id)) \
        .withColumn("_updated_at", current_timestamp())
    
    if source_file:
        df_with_audit = df_with_audit.withColumn("_source_file", lit(source_file))
    
    return df_with_audit


def get_latest_batch_id(spark: SparkSession, table_name: str) -> Optional[str]:
    """
    Get the latest batch ID from a table
    
    Args:
        spark: SparkSession
        table_name: Table name to check
    
    Returns:
        Latest batch ID or None
    """
    try:
        latest_batch = spark.sql(f"""
            SELECT _batch_id
            FROM {table_name}
            ORDER BY _ingestion_date DESC, _updated_at DESC
            LIMIT 1
        """).collect()
        
        if latest_batch:
            return latest_batch[0]["_batch_id"]
        return None
    except Exception:
        return None


# ============================================================================
# Data Cleaning Helpers
# ============================================================================

def clean_string_columns(df: DataFrame, 
                        columns: List[str]) -> DataFrame:
    """
    Clean string columns by trimming whitespace
    
    Args:
        df: DataFrame to clean
        columns: List of column names to clean
    
    Returns:
        DataFrame with cleaned columns
    """
    cleaned_df = df
    
    for column in columns:
        if column in df.columns:
            cleaned_df = cleaned_df.withColumn(
                column,
                trim(col(column))
            )
    
    return cleaned_df


def standardize_string_case(df: DataFrame,
                           columns: List[str],
                           case: str = "upper") -> DataFrame:
    """
    Standardize string case (upper or lower)
    
    Args:
        df: DataFrame to standardize
        columns: List of column names to standardize
        case: "upper" or "lower"
    
    Returns:
        DataFrame with standardized columns
    """
    standardized_df = df
    
    for column in columns:
        if column in df.columns:
            if case == "upper":
                standardized_df = standardized_df.withColumn(
                    column,
                    upper(col(column))
                )
            elif case == "lower":
                standardized_df = standardized_df.withColumn(
                    column,
                    lower(col(column))
                )
    
    return standardized_df


def handle_nulls(df: DataFrame,
                column_defaults: Dict[str, any]) -> DataFrame:
    """
    Handle null values with default values
    
    Args:
        df: DataFrame to handle nulls in
        column_defaults: Dictionary mapping column names to default values
    
    Returns:
        DataFrame with nulls handled
    """
    handled_df = df
    
    for column, default_value in column_defaults.items():
        if column in df.columns:
            handled_df = handled_df.withColumn(
                column,
                coalesce(col(column), lit(default_value))
            )
    
    return handled_df


def remove_duplicates(df: DataFrame,
                     key_columns: List[str],
                     keep: str = "first") -> DataFrame:
    """
    Remove duplicate rows based on key columns
    
    Args:
        df: DataFrame to deduplicate
        key_columns: List of columns that define uniqueness
        keep: "first" or "last" - which duplicate to keep
    
    Returns:
        DataFrame with duplicates removed
    """
    from pyspark.sql.window import Window
    
    # Check if _updated_at column exists, if not use _ingestion_date or add timestamp
    order_column = None
    if "_updated_at" in df.columns:
        order_column = col("_updated_at")
    elif "_ingestion_date" in df.columns:
        order_column = col("_ingestion_date")
    else:
        # Add timestamp column for ordering
        df = df.withColumn("_temp_timestamp", current_timestamp())
        order_column = col("_temp_timestamp")
    
    if keep == "first":
        window = Window.partitionBy(key_columns).orderBy(order_column.asc())
    else:
        window = Window.partitionBy(key_columns).orderBy(order_column.desc())
    
    # Add row number
    df_with_row_num = df.withColumn("_row_num", row_number().over(window))
    
    # Keep first or last row
    deduplicated_df = df_with_row_num.filter(col("_row_num") == 1).drop("_row_num")
    
    # Drop temporary column if it was added
    if "_temp_timestamp" in deduplicated_df.columns:
        deduplicated_df = deduplicated_df.drop("_temp_timestamp")
    
    return deduplicated_df


# ============================================================================
# Data Type Conversion Helpers
# ============================================================================

def convert_data_types(df: DataFrame,
                      type_mapping: Dict[str, str]) -> DataFrame:
    """
    Convert data types of columns
    
    Args:
        df: DataFrame to convert
        type_mapping: Dictionary mapping column names to target types
    
    Returns:
        DataFrame with converted data types
    """
    converted_df = df
    
    for column, target_type in type_mapping.items():
        if column in df.columns:
            converted_df = converted_df.withColumn(
                column,
                col(column).cast(target_type)
            )
    
    return converted_df


# ============================================================================
# Table Management Helpers
# ============================================================================

def create_table_if_not_exists(spark: SparkSession,
                              table_name: str,
                              schema: StructType,
                              partition_columns: List[str] = None):
    """
    Create a Delta table if it doesn't exist
    
    Args:
        spark: SparkSession
        table_name: Full table name (catalog.schema.table)
        schema: Schema for the table
        partition_columns: List of partition column names (optional)
    """
    try:
        # Check if table exists
        spark.sql(f"DESCRIBE TABLE {table_name}")
        print(f"Table {table_name} already exists")
    except Exception:
        # Create table
        schema_str = ", ".join([f"{field.name} {field.dataType}" for field in schema.fields])
        
        partition_clause = ""
        if partition_columns:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})"
        
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_str}
            )
            USING DELTA
            {partition_clause}
        """
        
        spark.sql(create_table_sql)
        print(f"Created table {table_name}")


def merge_data(spark: SparkSession,
              target_table: str,
              source_df: DataFrame,
              key_columns: List[str],
              update_columns: List[str] = None):
    """
    Merge data from source DataFrame into target table
    
    Args:
        spark: SparkSession
        target_table: Target table name
        source_df: Source DataFrame
        key_columns: List of key columns for matching
        update_columns: List of columns to update (if None, update all)
    """
    # Create temporary view
    source_df.createOrReplaceTempView("source_data")
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
    
    # Build update set
    if update_columns:
        update_set = ", ".join([f"{col} = source.{col}" for col in update_columns])
    else:
        # Get all columns except key columns
        all_columns = source_df.columns
        update_columns = [col for col in all_columns if col not in key_columns]
        update_set = ", ".join([f"{col} = source.{col}" for col in update_columns])
    
    # Build insert columns
    insert_columns = ", ".join(source_df.columns)
    insert_values = ", ".join([f"source.{col}" for col in source_df.columns])
    
    # Merge SQL
    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING source_data AS source
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
    """
    
    spark.sql(merge_sql)
    print(f"Merged data into {target_table}")


# ============================================================================
# Logging Helpers
# ============================================================================

def log_processing_stats(spark: SparkSession,
                        table_name: str,
                        batch_id: str,
                        row_count: int,
                        processing_time: float):
    """
    Log processing statistics
    
    Args:
        spark: SparkSession
        table_name: Table name that was processed
        batch_id: Batch ID for the current run
        row_count: Number of rows processed
        processing_time: Processing time in seconds
    """
    print(f"""
    ========================================
    Processing Statistics
    ========================================
    Table: {table_name}
    Batch ID: {batch_id}
    Rows Processed: {row_count}
    Processing Time: {processing_time:.2f} seconds
    ========================================
    """)

