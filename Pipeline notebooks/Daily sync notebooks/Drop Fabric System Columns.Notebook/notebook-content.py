# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4aee8a32-be91-489f-89f3-1a819b188807",
# META       "default_lakehouse_name": "Master_Bronze",
# META       "default_lakehouse_workspace_id": "b0f83c07-a701-49bb-a165-e06ca0ee4000",
# META       "known_lakehouses": [
# META         {
# META           "id": "4aee8a32-be91-489f-89f3-1a819b188807"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

table_name = "default_table"
schema_name = "default_schema"
pipeline_run_id = "default_run_id"
pipeline_trigger_time = "2024-01-01T00:00:00Z"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL
# table_name = "default_table"
# schema_name = "default_schema"
# pipeline_run_id = "default_run_id"
# pipeline_trigger_time = "2024-01-01T00:00:00Z"

# CELL 1 - Drop Link to Fabric System Columns
import json
import uuid
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Link to Fabric adds these system columns
# We KEEP: SinkCreatedOn, SinkModifiedOn (useful for tracking when data arrived in Fabric)
# We DROP: Id, IsDelete, PartitionId (we have our own tracking columns)
FABRIC_SYSTEM_COLUMNS_TO_DROP = ['Id', 'IsDelete', 'PartitionId']

results = {
    "status": "success",
    "operation": "DropFabricSystemColumns",
    "columns_dropped": 0,
    "columns_dropped_list": [],
    "columns_not_found": [],
    "message": ""
}

execution_start = datetime.now()
safe_table_name = table_name.replace("'", "''")
safe_schema_name = schema_name.replace("'", "''")
full_table_name = f"{safe_schema_name}.{safe_table_name}"
safe_pipeline_run_id = pipeline_run_id.replace("'", "''")

try:
    print(f"Checking for Fabric system columns in {full_table_name}")
    print(f"Columns to drop: {', '.join(FABRIC_SYSTEM_COLUMNS_TO_DROP)}")
    print(f"Columns to keep: SinkCreatedOn, SinkModifiedOn")
    
    # Step 1: Get current table schema
    print(f"\n--- Step 1: Get current table schema ---")
    current_schema = spark.sql(f"DESCRIBE {full_table_name}").collect()
    
    # Create map of existing columns (case-insensitive)
    existing_columns = {}
    for row in current_schema:
        col_name = row['col_name']
        if col_name and not col_name.startswith('#') and col_name.strip():
            existing_columns[col_name.lower()] = col_name
    
    print(f"Table has {len(existing_columns)} columns")
    
    # Step 2: Find which Fabric system columns exist
    print(f"\n--- Step 2: Check which system columns exist ---")
    columns_to_drop = []
    
    for system_col in FABRIC_SYSTEM_COLUMNS_TO_DROP:
        if system_col.lower() in existing_columns:
            actual_col_name = existing_columns[system_col.lower()]
            columns_to_drop.append(actual_col_name)
            print(f"  ✓ Found (will drop): {actual_col_name}")
        else:
            results["columns_not_found"].append(system_col)
            print(f"  - Not found: {system_col}")
    
    # Step 3: Drop the columns if any exist
    if columns_to_drop:
        print(f"\n--- Step 3: Enable Delta column mapping (required for DROP COLUMN) ---")
        
        try:
            # Enable column mapping mode for the table
            enable_mapping_sql = f"""
            ALTER TABLE {full_table_name} SET TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5'
            )
            """
            print(f"Enabling column mapping for {full_table_name}")
            spark.sql(enable_mapping_sql)
            print("✓ Column mapping enabled")
        except Exception as e:
            print(f"⚠ Column mapping may already be enabled or failed: {e}")
            # Continue anyway - it might already be enabled
        
        print(f"\n--- Step 4: Dropping {len(columns_to_drop)} Fabric system columns ---")
        
        for col_name in columns_to_drop:
            try:
                drop_sql = f"ALTER TABLE {full_table_name} DROP COLUMN {col_name}"
                print(f"Executing: {drop_sql}")
                spark.sql(drop_sql)
                results["columns_dropped_list"].append(col_name)
                print(f"  ✓ Dropped: {col_name}")
            except Exception as e:
                print(f"  ✗ Failed to drop {col_name}: {e}")
                # Add to error tracking
                if "error_details" not in results:
                    results["error_details"] = []
                results["error_details"].append({
                    "column": col_name,
                    "error": str(e)
                })
        
        results["columns_dropped"] = len(results["columns_dropped_list"])
        
        if results["columns_dropped"] > 0:
            results["message"] = f"Dropped {len(results['columns_dropped_list'])} Fabric system columns"
            print(f"\n{'='*60}")
            print(f"✓ Successfully cleaned up Fabric system columns")
            print(f"{'='*60}")
        else:
            results["status"] = "error"
            results["message"] = "Failed to drop any columns"
            print(f"\n{'='*60}")
            print(f"✗ Failed to drop columns - see error details")
            print(f"{'='*60}")
    else:
        print(f"\n--- Step 3: No Fabric system columns found ---")
        results["message"] = "No Fabric system columns found in table"
        print("✓ Table already clean - no action needed")
    
    # Step 5: Verify final state
    print(f"\n--- Step 5: Verify final table schema ---")
    final_schema = spark.sql(f"DESCRIBE {full_table_name}").collect()
    final_columns = [row['col_name'] for row in final_schema 
                     if row['col_name'] and not row['col_name'].startswith('#') 
                     and row['col_name'].strip()]
    
    print(f"Final column count: {len(final_columns)}")
    
    # Check if any system columns that should be dropped remain
    remaining_system = []
    for system_col in FABRIC_SYSTEM_COLUMNS_TO_DROP:
        if any(col.lower() == system_col.lower() for col in final_columns):
            remaining_system.append(system_col)
    
    if remaining_system:
        print(f"⚠ WARNING: Some system columns still exist: {', '.join(remaining_system)}")
        results["status"] = "error"  # Changed from warning to error
        results["message"] = f"Failed to drop {len(remaining_system)} columns: {', '.join(remaining_system)}"
    elif results["columns_dropped"] > 0:
        print(f"✓ All targeted system columns successfully removed")
        results["status"] = "success"
    
    execution_end = datetime.now()
    
    # Step 6: Log to metadata.SyncAuditLog
    print(f"\n--- Step 6: Logging to metadata.SyncAuditLog ---")
    
    try:
        sync_audit_schema = StructType([
            StructField("LogId", StringType(), False),
            StructField("PipelineRunId", StringType(), False),
            StructField("PipelineName", StringType(), False),
            StructField("TableName", StringType(), True),
            StructField("Operation", StringType(), False),
            StructField("StartTime", TimestampType(), False),
            StructField("EndTime", TimestampType(), True),
            StructField("RowsProcessed", IntegerType(), True),
            StructField("RowsDeleted", IntegerType(), True),
            StructField("RowsPurged", IntegerType(), True),
            StructField("Status", StringType(), False),
            StructField("ErrorMessage", StringType(), True),
            StructField("Notes", StringType(), True),
            StructField("RetryCount", IntegerType(), True),
            StructField("CreatedDate", TimestampType(), True)
        ])
        
        # Determine log status
        log_status = "Success" if results["status"] == "success" else "Error"
        error_message = None if results["status"] == "success" else results["message"]
        
        # Create details message
        if results["columns_dropped"] > 0:
            details = f"Dropped {results['columns_dropped']} columns: {', '.join(results['columns_dropped_list'])}"
        elif results["status"] == "success":
            details = "No Fabric system columns found"
        else:
            details = results["message"]
        
        log_entry = [(
            str(uuid.uuid4()),
            safe_pipeline_run_id,
            'DailySync',
            full_table_name,
            'DropFabricSystemColumns',
            execution_start,
            execution_end,
            results["columns_dropped"],
            0,
            0,
            log_status,
            None,
            details[:1000] if error_message is None else error_message[:1000],
            0,
            execution_end
        )]
        
        log_df = spark.createDataFrame(log_entry, sync_audit_schema)
        log_df.write.format("delta").mode("append").saveAsTable("metadata.SyncAuditLog")
        print(f"✓ Logged operation to metadata.SyncAuditLog")
        
    except Exception as log_error:
        print(f"⚠ Failed to log to SyncAuditLog: {log_error}")
        # Don't fail the whole operation if logging fails
    
except Exception as e:
    execution_end = datetime.now()
    results["status"] = "error"
    results["message"] = str(e)
    print(f"\nERROR: {e}")
    
    # Log the error to SyncAuditLog
    try:
        error_log = [(
            str(uuid.uuid4()),
            safe_pipeline_run_id,
            'DailySync',
            full_table_name,
            'DropFabricSystemColumns',
            execution_start,
            execution_end,
            0,
            0,
            0,
            'Error',
            str(e)[:1000],
            None,
            0,
            execution_end
        )]
        
        sync_audit_schema = StructType([
            StructField("LogId", StringType(), False),
            StructField("PipelineRunId", StringType(), False),
            StructField("PipelineName", StringType(), False),
            StructField("TableName", StringType(), True),
            StructField("Operation", StringType(), False),
            StructField("StartTime", TimestampType(), False),
            StructField("EndTime", TimestampType(), True),
            StructField("RowsProcessed", IntegerType(), True),
            StructField("RowsDeleted", IntegerType(), True),
            StructField("RowsPurged", IntegerType(), True),
            StructField("Status", StringType(), False),
            StructField("ErrorMessage", StringType(), True),
            StructField("Notes", StringType(), True),
            StructField("RetryCount", IntegerType(), True),
            StructField("CreatedDate", TimestampType(), True)
        ])
        
        error_df = spark.createDataFrame(error_log, sync_audit_schema)
        error_df.write.format("delta").mode("append").saveAsTable("metadata.SyncAuditLog")
        print("✓ Error logged to SyncAuditLog")
    except Exception as log_error:
        print(f"Failed to log error to SyncAuditLog: {log_error}")
    
    raise e

# Return results
print(f"\nResults: {results}")
mssparkutils.notebook.exit(json.dumps(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
