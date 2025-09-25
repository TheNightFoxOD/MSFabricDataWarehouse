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

# Parameter cell - mark this cell as parameter cell
table_name = "PARAM_NOT_SET_table_name"
schema_name = "PARAM_NOT_SET_schema_name"
pipeline_run_id = "PARAM_NOT_SET_pipeline_run_id"
pipeline_trigger_time = "PARAM_NOT_SET_pipeline_trigger_time"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

########################################
# Parameter cell - mark this cell as parameter cell
# table_name = "account"
# schema_name = "dataverse"
# pipeline_run_id = "e13e5a80-cba7-4305-8002-c923f1ca61ef"
# pipeline_trigger_time = "2025-09-23T10:54:15Z"
########################################

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import uuid
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *

full_table_name = f"{schema_name}.{table_name}"
execution_results = {
    "status": "success",
    "full_table_name": full_table_name,
    "logs_written": {},
    "errors": []
}

print(f"Final Summary for table: {full_table_name}")
print(f"Pipeline Run ID: {pipeline_run_id}")

# Query what operations were logged for this table/pipeline run
try:
    query = f"""
    SELECT Operation, Status, RowsProcessed, RowsDeleted, RowsPurged, StartTime, EndTime, ErrorMessage
    FROM metadata.SyncAuditLog 
    WHERE PipelineRunId = '{pipeline_run_id}' 
      AND TableName = '{full_table_name}'
    ORDER BY CreatedDate
    """
    
    log_results = spark.sql(query).collect()
    print(f"Found {len(log_results)} logged operations for this table")
    
    for row in log_results:
        print(f"- {row['Operation']}: {row['Status']} (Processed: {row['RowsProcessed']}, Deleted: {row['RowsDeleted']}, Purged: {row['RowsPurged']})")

except Exception as e:
    execution_results["errors"].append(f"Error querying logged operations: {str(e)}")
    print(f"Error querying logged operations: {e}")
    log_results = []

# Generate timestamps
end_time = datetime.now(timezone.utc)
try:
    if not pipeline_trigger_time.startswith('PARAM_NOT_SET'):
        timestamp_str = pipeline_trigger_time.replace('Z', '+00:00')
        import re
        timestamp_str = re.sub(r'\.(\d{6})\d*', r'.\1', timestamp_str)
        start_time = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    else:
        start_time = end_time
except:
    start_time = end_time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Determine what happened based on logged operations
operations_logged = {row['Operation']: row for row in log_results}

sync_type = None
total_records = 0
deleted_records = 0
purged_records = 0
overall_status = "error"

# Determine sync type and record counts from logged operations
if 'InitialSync' in operations_logged:
    sync_type = 'initial'
    total_records = operations_logged['InitialSync']['RowsProcessed'] or 0
elif 'DataSync' in operations_logged:
    sync_type = 'daily' 
    total_records = operations_logged['DataSync']['RowsProcessed'] or 0

if 'DeleteDetection' in operations_logged:
    deleted_records = operations_logged['DeleteDetection']['RowsDeleted'] or 0

if 'PurgeDetection' in operations_logged:  # When implemented
    purged_records = operations_logged['PurgeDetection']['RowsPurged'] or 0

# Determine overall status
if log_results:
    # Success if we have operations and no critical errors
    success_ops = [op for op in operations_logged.values() if op['Status'] == 'Success']
    error_ops = [op for op in operations_logged.values() if op['Status'] == 'Error']
    
    if success_ops and not error_ops:
        overall_status = "success"
    elif success_ops and error_ops:
        overall_status = "partial_success"
    else:
        overall_status = "error"
else:
    overall_status = "no_operations"

# Reconstruct sync_operations equivalent
sync_operations = {
    "status": overall_status,
    "sync_type": sync_type or "unknown",
    "total_records": total_records,
    "deleted_records": deleted_records,
    "purged_records": purged_records,
    "operations_logged": list(operations_logged.keys())
}

print(f"Reconstructed execution state:")
print(f"- Overall Status: {overall_status}")
print(f"- Sync Type: {sync_type}")
print(f"- Total Records: {total_records}")
print(f"- Deleted Records: {deleted_records}")
print(f"- Operations: {list(operations_logged.keys())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DataValidation entry - only if we had successful sync operations
if sync_operations["status"] in ["success", "partial_success"] and sync_operations["total_records"] > 0:
    validation_schema = StructType([
        StructField("ValidationId", StringType(), False),
        StructField("ValidationDate", TimestampType(), False),
        StructField("TableName", StringType(), False),
        StructField("SourceRowCount", IntegerType(), True),
        StructField("BronzeRowCount", IntegerType(), False),
        StructField("ActiveRowCount", IntegerType(), False),
        StructField("DeletedRowCount", IntegerType(), False),
        StructField("PurgedRowCount", IntegerType(), False),
        StructField("ValidationPassed", BooleanType(), False)
    ])
    
    try:
        # Query Bronze layer for actual current state
        bronze_counts_query = f"""
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN COALESCE(IsDeleted, false) = false AND COALESCE(IsPurged, false) = false THEN 1 ELSE 0 END) as active_records,
            SUM(CASE WHEN COALESCE(IsDeleted, false) = true THEN 1 ELSE 0 END) as deleted_records,
            SUM(CASE WHEN COALESCE(IsPurged, false) = true THEN 1 ELSE 0 END) as purged_records
        FROM {full_table_name}
        """
        
        bronze_counts = spark.sql(bronze_counts_query).collect()[0]
        
        total_records_bronze = bronze_counts['total_records']
        active_records = bronze_counts['active_records'] 
        deleted_records_bronze = bronze_counts['deleted_records']
        purged_records_bronze = bronze_counts['purged_records']
        
        # Determine expected source count
        if sync_operations["sync_type"] == "initial":
            expected_source_count = sync_operations["total_records"]
        else:
            expected_source_count = active_records
        
        # Validation checks
        validation_errors = []
        
        # Internal consistency check
        calculated_total = active_records + deleted_records_bronze + purged_records_bronze
        if total_records_bronze != calculated_total:
            validation_errors.append(f"Total mismatch: {total_records_bronze} != {calculated_total}")
        
        # For initial sync: validate we got expected records
        if sync_operations["sync_type"] == "initial":
            if active_records != sync_operations["total_records"]:
                validation_errors.append(f"Initial sync mismatch: active={active_records}, copied={sync_operations['total_records']}")
        
        validation_passed = len(validation_errors) == 0
        
        if validation_errors:
            print(f"Validation errors: {validation_errors}")
            execution_results["errors"].extend(validation_errors)
        
        validation_entry = [(
            str(uuid.uuid4()), 
            end_time, 
            full_table_name, 
            expected_source_count,
            total_records_bronze,
            active_records,
            deleted_records_bronze,
            purged_records_bronze,
            validation_passed
        )]
        
        validation_df = spark.createDataFrame(validation_entry, validation_schema)
        validation_df.write.format("delta").mode("append").saveAsTable("metadata.DataValidation")
        execution_results["logs_written"]["data_validation"] = 1
        
        print(f"DataValidation: source={expected_source_count}, total={total_records_bronze}, active={active_records}, passed={validation_passed}")
        
    except Exception as e:
        execution_results["errors"].append(f"DataValidation query failed: {str(e)}")
        print(f"DataValidation failed: {e}")
else:
    print("Skipping DataValidation (no successful sync operations)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update LastDailySync for successful operations
config_updated = False
if sync_operations["status"] in ["success", "partial_success"]:
    try:
        spark.sql(f"""
            UPDATE metadata.PipelineConfig 
            SET LastDailySync = '{end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' 
            WHERE TableName = '{table_name}'
        """)
        
        execution_results["logs_written"]["pipeline_config_update"] = 1
        config_updated = True
        print(f"Updated metadata.PipelineConfig.LastDailySync for {table_name}")
        
    except Exception as e:
        execution_results["errors"].append(f"Error updating PipelineConfig: {str(e)}")
        print(f"Error updating PipelineConfig: {e}")
else:
    print("Skipping PipelineConfig update (no successful sync)")

# Create Checkpoint Entry
checkpoint_created = False
if sync_operations["status"] == "success" and sync_operations["total_records"] > 0:
    checkpoint_schema = StructType([
        StructField("CheckpointId", StringType(), False),
        StructField("CheckpointName", StringType(), False),
        StructField("CheckpointType", StringType(), False),
        StructField("CreatedDate", TimestampType(), False),
        StructField("TablesIncluded", IntegerType(), False),
        StructField("TotalRows", LongType(), True),
        StructField("ValidationStatus", StringType(), False),
        StructField("RetentionDate", DateType(), False),
        StructField("IsActive", BooleanType(), False)
    ])
    
    try:
        retention_date = (end_time + timedelta(days=7)).date()
        
        checkpoint_entry = [(
            str(uuid.uuid4()),
            f"bronze_backup_{end_time.strftime('%Y-%m-%d')}",
            'Daily',
            end_time,
            1,
            sync_operations["total_records"],
            'Validated',
            retention_date,
            True
        )]
        
        checkpoint_df = spark.createDataFrame(checkpoint_entry, checkpoint_schema)
        checkpoint_df.write.format("delta").mode("append").saveAsTable("metadata.CheckpointHistory")
        execution_results["logs_written"]["checkpoint_history"] = 1
        checkpoint_created = True
        print(f"Created checkpoint entry")
        
    except Exception as e:
        execution_results["errors"].append(f"Error creating checkpoint: {str(e)}")
        print(f"Error creating checkpoint: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate final results
execution_results.update({
    "total_logs_written": sum(execution_results["logs_written"].values()),
    "sync_operations": sync_operations
})

# Print comprehensive summary
print(f"\n=== Complete Execution Summary ===")
print(f"Table: {full_table_name}")
print(f"Pipeline Run: {pipeline_run_id}")
print(f"Operations Logged: {sync_operations['operations_logged']}")
print(f"Sync Type: {sync_operations['sync_type']}")
print(f"Records Processed: {sync_operations['total_records']}")
print(f"Records Deleted: {sync_operations['deleted_records']}")
print(f"Records Purged: {sync_operations['purged_records']}")
print(f"Overall Status: {sync_operations['status']}")
print(f"PipelineConfig Updated: {'Yes' if config_updated else 'No'}")
print(f"Checkpoint Created: {'Yes' if checkpoint_created else 'No'}")
print(f"Total Metadata Writes: {execution_results['total_logs_written']}")

if execution_results["errors"]:
    print(f"Errors Encountered: {execution_results['errors']}")

# Return results
mssparkutils.notebook.exit(json.dumps(execution_results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
