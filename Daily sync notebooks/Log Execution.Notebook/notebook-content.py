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
    "errors": [],
    "operation_results": {}  # Track individual operation outcomes
}

print(f"Final Summary for table: {full_table_name}")
print(f"Pipeline Run ID: {pipeline_run_id}")

# Query what operations were logged for this table/pipeline run
try:
    query = f"""
    SELECT Operation, Status, RowsProcessed, RowsDeleted, RowsPurged, StartTime, EndTime, ErrorMessage, Notes
    FROM metadata.SyncAuditLog 
    WHERE PipelineRunId = '{pipeline_run_id}' 
    AND TableName = '{full_table_name}'
    ORDER BY CreatedDate
    """
    
    log_results = spark.sql(query).collect()
    print(f"Found {len(log_results)} logged operations for this table")
    
    # Parse and display each operation with clear success/failure status
    for row in log_results:
        operation = row['Operation']
        status = row['Status']
        error_msg = row['ErrorMessage']
        notes = row['Notes']
        
        # Track operation results
        execution_results["operation_results"][operation] = {
            "status": status,
            "rows_processed": row['RowsProcessed'] or 0,
            "rows_deleted": row['RowsDeleted'] or 0,
            "rows_purged": row['RowsPurged'] or 0,
            "error_message": error_msg,
            "notes": row['Notes']
        }
        
        if status == "Success":
            # Always show both counters and notes for all operations
            notes_display = f" - {notes}" if notes else ""
            print(f"✅ {operation}: SUCCESS (Processed: {row['RowsProcessed']}, Deleted: {row['RowsDeleted']}, Purged: {row['RowsPurged']}){notes_display}")
        elif status == "Error":
            print(f"❌ {operation}: FAILED - {error_msg}")
            execution_results["errors"].append(f"{operation} failed: {error_msg}")
        else:
            notes_display = f" - {notes}" if notes else ""
            print(f"⚠️  {operation}: {status} (Processed: {row['RowsProcessed']}, Deleted: {row['RowsDeleted']}, Purged: {row['RowsPurged']}){notes_display}")
            
except Exception as e:
    execution_results["errors"].append(f"Error querying logged operations: {str(e)}")
    print(f"❌ Error querying logged operations: {e}")
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

# Determine what happened based on logged operations with detailed error analysis
operations_logged = {row['Operation']: row for row in log_results}

sync_type = None
upserted_records = 0
deleted_records = 0
purged_records = 0
overall_status = "error"
failed_operations = []
successful_operations = []

# Analyze each operation type
for operation, details in execution_results["operation_results"].items():
    if details["status"] == "Success":
        successful_operations.append(operation)
        if operation == "CreateTable":
            sync_type = 'initial'
            upserted_records += details["rows_processed"]
        elif operation == "DataSync":
            sync_type = 'daily'
            upserted_records += details["rows_processed"]
        elif operation == "DeleteDetection":
            deleted_records += details["rows_deleted"]
            purged_records += details["rows_purged"]
    else:
        failed_operations.append({
            "operation": operation,
            "error": details["error_message"]
        })

# Determine overall status based on critical vs non-critical failures
critical_operations = ["CreateTable", "DataSync", "ColumnAddition", "ColumnDrop"]  # These are essential for sync
critical_failures = [op for op in failed_operations if op["operation"] in critical_operations]

if not log_results:
    overall_status = "no_operations"
elif critical_failures:
    overall_status = "critical_failure"
elif failed_operations and successful_operations:
    overall_status = "partial_success"
elif successful_operations and not failed_operations:
    overall_status = "success"
else:
    overall_status = "complete_failure"

# Enhanced sync_operations with error details
sync_operations = {
    "status": overall_status,
    "sync_type": sync_type or "unknown",
    "upserted_records": upserted_records,
    "deleted_records": deleted_records,
    "purged_records": purged_records,
    "operations_logged": list(operations_logged.keys()),
    "successful_operations": successful_operations,
    "failed_operations": failed_operations,
    "critical_failures": critical_failures
}

print(f"\n=== Execution State Analysis ===")
print(f"Overall Status: {overall_status}")
print(f"Sync Type: {sync_type}")
print(f"Successful Operations: {successful_operations}")
if failed_operations:
    print(f"Failed Operations:")
    for failure in failed_operations:
        print(f"  ❌ {failure['operation']}: {failure['error']}")
if critical_failures:
    print(f"⚠️  CRITICAL FAILURES DETECTED: {[f['operation'] for f in critical_failures]}")
print(f"Records - Upserted: {upserted_records}, Deleted: {deleted_records}, Purged: {purged_records}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DataValidation entry - consider failed operations in validation logic
validation_attempted = False
validation_passed = False

if sync_operations["status"] in ["success", "partial_success"] and sync_operations["upserted_records"] > 0:
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
        validation_attempted = True
        
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
            expected_source_count = sync_operations["upserted_records"]
        else:
            expected_source_count = active_records
        
        # Validation checks with error consideration
        validation_errors = []
        validation_notes = []
        
        # Internal consistency check
        calculated_total = active_records + deleted_records_bronze + purged_records_bronze
        if total_records_bronze != calculated_total:
            validation_errors.append(f"Total mismatch: {total_records_bronze} != {calculated_total}")
        
        # For initial sync: validate we got expected records
        if sync_operations["sync_type"] == "initial":
            if active_records != sync_operations["upserted_records"]:
                validation_errors.append(f"Initial sync mismatch: active={active_records}, copied={sync_operations['upserted_records']}")
        
        # Add notes about failed operations (just for logging, not stored)
        if sync_operations["failed_operations"]:
            failed_ops = [f["operation"] for f in sync_operations["failed_operations"]]
            validation_notes.append(f"Operations failed: {', '.join(failed_ops)}")
            print(f"ℹ️  Note: Some operations failed: {', '.join(failed_ops)}")
        
        validation_passed = len(validation_errors) == 0
        
        if validation_errors:
            print(f"❌ Validation FAILED:")
            for error in validation_errors:
                print(f"   - {error}")
            execution_results["errors"].extend(validation_errors)
        else:
            print(f"✅ Validation PASSED")
        
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
        
        print(f"DataValidation Results: source={expected_source_count}, total={total_records_bronze}, active={active_records}, passed={validation_passed}")
        
    except Exception as e:
        execution_results["errors"].append(f"DataValidation query failed: {str(e)}")
        print(f"❌ DataValidation FAILED: {e}")
        validation_attempted = False
else:
    print("⏭️  Skipping DataValidation (no successful sync operations or no records processed)")

# Update execution results
execution_results["validation_attempted"] = validation_attempted
execution_results["validation_passed"] = validation_passed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update LastDailySync only for successful operations
config_updated = False
checkpoint_created = False

if sync_operations["status"] in ["success", "partial_success"] and not sync_operations["critical_failures"]:
    try:
        spark.sql(f"""
            UPDATE metadata.PipelineConfig 
            SET LastDailySync = '{end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' 
            WHERE TableName = '{table_name}'
        """)
        
        execution_results["logs_written"]["pipeline_config_update"] = 1
        config_updated = True
        print(f"✅ Updated metadata.PipelineConfig.LastDailySync for {table_name}")
        
    except Exception as e:
        execution_results["errors"].append(f"Error updating PipelineConfig: {str(e)}")
        print(f"❌ Error updating PipelineConfig: {e}")
else:
    reason = "critical failures detected" if sync_operations["critical_failures"] else "no successful sync operations"
    print(f"⏭️  Skipping PipelineConfig update ({reason})")

# Calculate total processed records (upserts + deletes + purges)
total_processed_records = (
    sync_operations["upserted_records"] + 
    sync_operations["deleted_records"] + 
    sync_operations["purged_records"]
)

# Create Checkpoint Entry - only for complete success without critical failures
if (sync_operations["status"] == "success" and 
    total_processed_records > 0 and 
    not sync_operations["critical_failures"] and
    validation_passed):
    
    checkpoint_schema = StructType([
        StructField("CheckpointId", StringType(), False),
        StructField("CheckpointName", StringType(), False),
        StructField("CheckpointType", StringType(), False),
        StructField("CreatedDate", TimestampType(), False),
        StructField("TablesIncluded", IntegerType(), False),
        StructField("TotalRows", LongType(), True),
        StructField("ValidationStatus", StringType(), False),
        StructField("RetentionDate", DateType(), False),
        StructField("IsActive", BooleanType(), False),
    ])
    
    try:
        retention_date = (end_time + timedelta(days=7)).date()
        
        checkpoint_entry = [(
            str(uuid.uuid4()),
            f"bronze_backup_{end_time.strftime('%Y-%m-%d')}",
            'Daily',
            end_time,
            1,
            total_processed_records,
            'Validated',
            retention_date,
            True
        )]
        
        checkpoint_df = spark.createDataFrame(checkpoint_entry, checkpoint_schema)
        checkpoint_df.write.format("delta").mode("append").saveAsTable("metadata.CheckpointHistory")
        execution_results["logs_written"]["checkpoint_history"] = 1
        checkpoint_created = True
        print(f"✅ Created checkpoint entry with {total_processed_records} total processed records")
        
    except Exception as e:
        execution_results["errors"].append(f"Error creating checkpoint: {str(e)}")
        print(f"❌ Error creating checkpoint: {e}")
else:
    # Explain why checkpoint wasn't created
    reasons = []
    if sync_operations["status"] != "success":
        reasons.append(f"status is {sync_operations['status']}")
    if total_processed_records <= 0:
        reasons.append("no records processed")
    if sync_operations["critical_failures"]:
        reasons.append("critical failures detected")
    if validation_attempted and not validation_passed:
        reasons.append("validation failed")
    
    print(f"⏭️  Skipping checkpoint creation ({', '.join(reasons)})")

# Update execution results
execution_results["config_updated"] = config_updated
execution_results["checkpoint_created"] = checkpoint_created

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate final results with detailed status reporting
execution_results.update({
    "total_logs_written": sum(execution_results["logs_written"].values()),
    "sync_operations": sync_operations
})

# Determine final execution status
if sync_operations["critical_failures"]:
    execution_results["status"] = "critical_failure"
elif execution_results["errors"]:
    execution_results["status"] = "partial_success"
else:
    execution_results["status"] = "success"

# Print comprehensive summary with clear success/failure indicators
print(f"\n" + "="*50)
print(f"📊 FINAL EXECUTION SUMMARY")
print(f"="*50)
print(f"Table: {full_table_name}")
print(f"Pipeline Run: {pipeline_run_id}")
print(f"Final Status: {execution_results['status'].upper()}")

print(f"\n🔄 OPERATIONS SUMMARY:")
for operation in sync_operations['operations_logged']:
    op_result = execution_results["operation_results"][operation]
    if op_result["status"] == "Success":
        print(f"  ✅ {operation}: SUCCESS")
    else:
        print(f"  ❌ {operation}: FAILED - {op_result['error_message']}")

print(f"\n📈 RECORD COUNTS:")
print(f"  • Upserted Records: {sync_operations['upserted_records']}")
print(f"  • Deleted Records: {sync_operations['deleted_records']}")
print(f"  • Purged Records: {sync_operations['purged_records']}")

print(f"\n🔧 METADATA UPDATES:")
print(f"  • DataValidation: {'✅ SUCCESS' if execution_results.get('validation_passed', False) else '❌ FAILED' if execution_results.get('validation_attempted', False) else '⏭️ SKIPPED'}")
print(f"  • PipelineConfig: {'✅ UPDATED' if execution_results.get('config_updated', False) else '⏭️ SKIPPED'}")
print(f"  • Checkpoint: {'✅ CREATED' if execution_results.get('checkpoint_created', False) else '⏭️ SKIPPED'}")
print(f"  • Total Writes: {execution_results['total_logs_written']}")

if execution_results["errors"]:
    print(f"\n⚠️  ERRORS ENCOUNTERED:")
    for i, error in enumerate(execution_results["errors"], 1):
        print(f"  {i}. {error}")

if sync_operations["critical_failures"]:
    print(f"\n🚨 CRITICAL FAILURES - IMMEDIATE ATTENTION REQUIRED:")
    for failure in sync_operations["critical_failures"]:
        print(f"  💥 {failure['operation']}: {failure['error']}")

print(f"\n" + "="*50)

# Return comprehensive results
mssparkutils.notebook.exit(json.dumps(execution_results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
