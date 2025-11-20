"""
Data Quality Framework for Databricks Medallion Architecture

This module provides reusable QC functions for data validation including:
- Row count validation
- Null checks
- Duplicate detection
- Schema validation
- Referential integrity checks
- Business rule validation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, when, isnan, isnull, lit, sum as spark_sum
from pyspark.sql.types import StructType
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json

# ============================================================================
# QC Result Structure
# ============================================================================

class QCResult:
    """Class to store QC validation results"""

    print('Inside QCResult class')
    
    def __init__(self, 
                 table_name: str,
                 check_name: str,
                 status: str,
                 message: str,
                 row_count: int = None,
                 failed_rows: int = None,
                 threshold: float = None,
                 actual_value: float = None):
        self.table_name = table_name
        self.check_name = check_name
        self.status = status  # PASS, FAIL, WARNING
        self.message = message
        self.row_count = row_count
        self.failed_rows = failed_rows
        self.threshold = threshold
        self.actual_value = actual_value
        self.timestamp = datetime.now()
    
    def to_dict(self):
        """Convert QC result to dictionary"""
        return {
            "table_name": self.table_name,
            "check_name": self.check_name,
            "status": self.status,
            "message": self.message,
            "row_count": self.row_count,
            "failed_rows": self.failed_rows,
            "threshold": self.threshold,
            "actual_value": self.actual_value,
            "timestamp": self.timestamp.isoformat()
        }
    
    def __repr__(self):
        return f"QCResult(table={self.table_name}, check={self.check_name}, status={self.status})"

# ============================================================================
# Basic QC Checks
# ============================================================================

def check_row_count(df: DataFrame, 
                   table_name: str,
                   min_rows: int = 1,
                   expected_rows: int = None) -> QCResult:
    """
    Check if DataFrame has minimum required rows
    Args:
        df: DataFrame to check
        table_name: Name of the table
        min_rows: Minimum number of rows required
        expected_rows: Expected number of rows (optional)
    
    Returns:
        QCResult object
    """
    row_count = df.count()
    
    if row_count < min_rows:
        return QCResult(
            table_name=table_name,
            check_name="row_count",
            status="FAIL",
            message=f"Row count {row_count} is below minimum threshold {min_rows}",
            row_count=row_count,
            threshold=min_rows
        )
    
    if expected_rows and row_count != expected_rows:
        return QCResult(
            table_name=table_name,
            check_name="row_count",
            status="WARNING",
            message=f"Row count {row_count} does not match expected {expected_rows}",
            row_count=row_count,
            threshold=expected_rows
        )
    
    return QCResult(
        table_name=table_name,
        check_name="row_count",
        status="PASS",
        message=f"Row count {row_count} is valid",
        row_count=row_count
    )


def check_nulls(df: DataFrame,
               table_name: str,
               columns: List[str],
               max_null_percentage: float = 0.1) -> List[QCResult]:
    """
    Check for null values in specified columns
    
    Args:
        df: DataFrame to check
        table_name: Name of the table
        columns: List of columns to check
        max_null_percentage: Maximum allowed null percentage (0.0 to 1.0)
    
    Returns:
        List of QCResult objects
    """
    results = []
    total_rows = df.count()
    
    for column in columns:
        if column not in df.columns:
            results.append(QCResult(
                table_name=table_name,
                check_name=f"null_check_{column}",
                status="FAIL",
                message=f"Column {column} not found in DataFrame"
            ))
            continue
        
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = null_count / total_rows if total_rows > 0 else 0.0
        
        if null_percentage > max_null_percentage:
            results.append(QCResult(
                table_name=table_name,
                check_name=f"null_check_{column}",
                status="FAIL",
                message=f"Null percentage {null_percentage:.2%} exceeds threshold {max_null_percentage:.2%}",
                row_count=total_rows,
                failed_rows=null_count,
                threshold=max_null_percentage,
                actual_value=null_percentage
            ))
        else:
            results.append(QCResult(
                table_name=table_name,
                check_name=f"null_check_{column}",
                status="PASS",
                message=f"Null percentage {null_percentage:.2%} is within threshold",
                row_count=total_rows,
                failed_rows=null_count,
                threshold=max_null_percentage,
                actual_value=null_percentage
            ))
    
    return results


def check_duplicates(df: DataFrame,
                    table_name: str,
                    key_columns: List[str],
                    max_duplicate_percentage: float = 0.05) -> QCResult:
    """
    Check for duplicate rows based on key columns
    
    Args:
        df: DataFrame to check
        table_name: Name of the table
        key_columns: List of columns that should be unique
        max_duplicate_percentage: Maximum allowed duplicate percentage
    
    Returns:
        QCResult object
    """
    total_rows = df.count()
    
    # Count duplicates
    duplicate_df = df.groupBy(key_columns).count().filter(col("count") > 1)
    duplicate_count = duplicate_df.count()
    
    if duplicate_count > 0:
        # Calculate total duplicate rows
        total_duplicates = duplicate_df.agg(spark_sum("count").alias("total")).collect()[0]["total"]
        duplicate_percentage = total_duplicates / total_rows if total_rows > 0 else 0.0
        
        if duplicate_percentage > max_duplicate_percentage:
            return QCResult(
                table_name=table_name,
                check_name="duplicate_check",
                status="FAIL",
                message=f"Duplicate percentage {duplicate_percentage:.2%} exceeds threshold {max_duplicate_percentage:.2%}",
                row_count=total_rows,
                failed_rows=total_duplicates,
                threshold=max_duplicate_percentage,
                actual_value=duplicate_percentage
            )
        else:
            return QCResult(
                table_name=table_name,
                check_name="duplicate_check",
                status="WARNING",
                message=f"Found {duplicate_count} duplicate key combinations",
                row_count=total_rows,
                failed_rows=total_duplicates,
                threshold=max_duplicate_percentage,
                actual_value=duplicate_percentage
            )
    
    return QCResult(
        table_name=table_name,
        check_name="duplicate_check",
        status="PASS",
        message="No duplicates found",
        row_count=total_rows
    )


def check_schema(df: DataFrame,
                table_name: str,
                expected_schema: StructType) -> QCResult:
    """
    Check if DataFrame schema matches expected schema
    
    Args:
        df: DataFrame to check
        table_name: Name of the table
        expected_schema: Expected schema (StructType)
    
    Returns:
        QCResult object
    """
    print('Inside check_schema -->')
    actual_schema = df.schema
    
    # Check column names
    expected_columns = set([field.name for field in expected_schema.fields])
    actual_columns = set(df.columns)
    
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns
    
    if missing_columns or extra_columns:
        message = []
        if missing_columns:
            message.append(f"Missing columns: {missing_columns}")
        if extra_columns:
            message.append(f"Extra columns: {extra_columns}")
        print('Calling QCResult -->')
        
        return QCResult(
            table_name=table_name,
            check_name="schema_check",
            status="FAIL",
            message="; ".join(message)
        )
    
    # Check data types (basic check)
    type_mismatches = []
    for field in expected_schema.fields:
        actual_field = [f for f in actual_schema.fields if f.name == field.name]
        if actual_field:
            if str(actual_field[0].dataType) != str(field.dataType):
                type_mismatches.append(f"{field.name}: expected {field.dataType}, got {actual_field[0].dataType}")
    
    if type_mismatches:
        return QCResult(
            table_name=table_name,
            check_name="schema_check",
            status="WARNING",
            message=f"Type mismatches: {', '.join(type_mismatches)}"
        )
    print('Calling QCResult -->')
    return QCResult(
        table_name=table_name,
        check_name="schema_check",
        status="PASS",
        message="Schema matches expected schema"
    )


# ============================================================================
# Referential Integrity Checks
# ============================================================================

def check_referential_integrity(df: DataFrame,
                               table_name: str,
                               foreign_key_column: str,
                               reference_table: DataFrame,
                               reference_key_column: str) -> QCResult:
    """
    Check referential integrity between tables
    
    Args:
        df: DataFrame with foreign key
        table_name: Name of the table with foreign key
        foreign_key_column: Foreign key column name
        reference_table: Reference DataFrame
        reference_key_column: Reference key column name
    
    Returns:
        QCResult object
    """
    total_rows = df.count()
    
    # Get valid keys from reference table
    valid_keys = reference_table.select(reference_key_column).distinct()
    
    # Find invalid foreign keys
    invalid_fk_df = df.join(
        valid_keys,
        df[foreign_key_column] == valid_keys[reference_key_column],
        "left_anti"
    ).filter(col(foreign_key_column).isNotNull())
    
    invalid_count = invalid_fk_df.count()
    
    if invalid_count > 0:
        invalid_percentage = invalid_count / total_rows if total_rows > 0 else 0.0
        
        return QCResult(
            table_name=table_name,
            check_name=f"referential_integrity_{foreign_key_column}",
            status="FAIL",
            message=f"Found {invalid_count} rows with invalid foreign keys",
            row_count=total_rows,
            failed_rows=invalid_count,
            actual_value=invalid_percentage
        )
    
    return QCResult(
        table_name=table_name,
        check_name=f"referential_integrity_{foreign_key_column}",
        status="PASS",
        message="All foreign keys are valid",
        row_count=total_rows
    )


# ============================================================================
# Business Rule Checks
# ============================================================================

def check_value_range(df: DataFrame,
                     table_name: str,
                     column: str,
                     min_value: float = None,
                     max_value: float = None) -> QCResult:
    """
    Check if values in a column are within specified range
    
    Args:
        df: DataFrame to check
        table_name: Name of the table
        column: Column name to check
        min_value: Minimum allowed value
        max_value: Maximum allowed value
    
    Returns:
        QCResult object
    """
    total_rows = df.count()
    
    if column not in df.columns:
        return QCResult(
            table_name=table_name,
            check_name=f"value_range_{column}",
            status="FAIL",
            message=f"Column {column} not found"
        )
    
    # Build filter condition
    conditions = []
    if min_value is not None:
        conditions.append(col(column) < min_value)
    if max_value is not None:
        conditions.append(col(column) > max_value)
    
    if not conditions:
        return QCResult(
            table_name=table_name,
            check_name=f"value_range_{column}",
            status="PASS",
            message="No range constraints specified"
        )
    
    # Find invalid values
    invalid_df = df.filter(
        when(col(column).isNotNull(), 
             conditions[0] if len(conditions) == 1 else 
             (conditions[0] | conditions[1]))
        .otherwise(lit(False))
    )
    
    invalid_count = invalid_df.count()
    
    if invalid_count > 0:
        range_str = f"[{min_value if min_value else '-∞'}, {max_value if max_value else '∞'}]"
        
        return QCResult(
            table_name=table_name,
            check_name=f"value_range_{column}",
            status="FAIL",
            message=f"Found {invalid_count} rows with values outside range {range_str}",
            row_count=total_rows,
            failed_rows=invalid_count,
            actual_value=invalid_count / total_rows if total_rows > 0 else 0.0
        )
    
    return QCResult(
        table_name=table_name,
        check_name=f"value_range_{column}",
        status="PASS",
        message=f"All values are within range [{min_value if min_value else '-∞'}, {max_value if max_value else '∞'}]",
        row_count=total_rows
    )


def check_valid_values(df: DataFrame,
                      table_name: str,
                      column: str,
                      valid_values: List[str]) -> QCResult:
    """
    Check if values in a column are in the list of valid values
    
    Args:
        df: DataFrame to check
        table_name: Name of the table
        column: Column name to check
        valid_values: List of valid values
    
    Returns:
        QCResult object
    """
    total_rows = df.count()
    
    if column not in df.columns:
        return QCResult(
            table_name=table_name,
            check_name=f"valid_values_{column}",
            status="FAIL",
            message=f"Column {column} not found"
        )
    
    # Find invalid values (excluding nulls)
    invalid_df = df.filter(
        col(column).isNotNull() & 
        ~col(column).isin(valid_values)
    )
    
    invalid_count = invalid_df.count()
    
    if invalid_count > 0:
        return QCResult(
            table_name=table_name,
            check_name=f"valid_values_{column}",
            status="FAIL",
            message=f"Found {invalid_count} rows with invalid values. Valid values: {valid_values}",
            row_count=total_rows,
            failed_rows=invalid_count,
            actual_value=invalid_count / total_rows if total_rows > 0 else 0.0
        )
    
    return QCResult(
        table_name=table_name,
        check_name=f"valid_values_{column}",
        status="PASS",
        message=f"All values are valid. Valid values: {valid_values}",
        row_count=total_rows
    )


# ============================================================================
# Comprehensive QC Runner
# ============================================================================

def run_qc_checks(df: DataFrame,
                 table_name: str,
                 checks: Dict[str, any]) -> List[QCResult]:
    """
    Run comprehensive QC checks on a DataFrame
    
    Args:
        df: DataFrame to check
        table_name: Name of the table
        checks: Dictionary of checks to run
    
    Returns:
        List of QCResult objects
    """
    results = []
    
    # Row count check
    if "row_count" in checks:
        min_rows = checks["row_count"].get("min_rows", 1)
        expected_rows = checks["row_count"].get("expected_rows")
        results.append(check_row_count(df, table_name, min_rows, expected_rows))
    
    # Null checks
    if "nulls" in checks:
        columns = checks["nulls"].get("columns", [])
        max_null_percentage = checks["nulls"].get("max_null_percentage", 0.1)
        results.extend(check_nulls(df, table_name, columns, max_null_percentage))
    
    # Duplicate checks
    if "duplicates" in checks:
        key_columns = checks["duplicates"].get("key_columns", [])
        max_duplicate_percentage = checks["duplicates"].get("max_duplicate_percentage", 0.05)
        results.append(check_duplicates(df, table_name, key_columns, max_duplicate_percentage))
    
    # Value range checks
    if "value_ranges" in checks:
        for column, range_config in checks["value_ranges"].items():
            min_value = range_config.get("min_value")
            max_value = range_config.get("max_value")
            results.append(check_value_range(df, table_name, column, min_value, max_value))
    
    # Valid values checks
    if "valid_values" in checks:
        for column, valid_values in checks["valid_values"].items():
            results.append(check_valid_values(df, table_name, column, valid_values))
    
    return results


# ============================================================================
# Save QC Results
# ============================================================================

def save_qc_results(spark: SparkSession,
                   results: List[QCResult],
                   results_table: str,
                   batch_id: str):
    """
    Save QC results to a Delta table
    
    Args:
        spark: SparkSession
        results: List of QCResult objects
        results_table: Full table name for storing results
        batch_id: Batch ID for the current run
    """
    if not results:
        return
    
    # Convert results to DataFrame
    results_data = [result.to_dict() for result in results]
    results_df = spark.createDataFrame(results_data)
    
    # Add batch_id
    results_df = results_df.withColumn("batch_id", lit(batch_id))
    results_df = results_df.withColumn("row_count", col("row_count").cast("int"))
    results_df = results_df.withColumn("failed_rows", col("failed_rows").cast("int"))
    results_df = results_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    results_df.printSchema()
    
    # Write to Delta table (append mode)
    results_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(results_table)
    
    print(f"Saved {len(results)} QC results to {results_table}")

