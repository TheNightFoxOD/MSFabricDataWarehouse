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

# Always-running activity results
schema_check_result = 'PARAM_NOT_SET_schema_check_result'

# Conditional schema management activities
table_creation_result = 'null'
tracking_columns_result = 'null'

# Conditional sync operation activities (Multiple inputs)
upsert_records_result = 'null'
append_records_result = 'null'
delete_detection_result = 'null'
purge_detection_result = 'null'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *

########################################
# Parameter cell - mark this cell as parameter cell
table_name = "od_donation"
schema_name = "dataverse"
pipeline_run_id = "f5cafc31-906e-4408-9610-559b5a1506c9"
pipeline_trigger_time = "2025-09-15T15:03:40.8104123Z"

# Always-running activity results
schema_check_result = '{\"table_exists\": false, \"schema_changed\": false, \"required_actions\": [\"CREATE_TABLE\", \"ADD_TRACKING_COLUMNS\"], \"current_columns\": [], \"error_message\": \"\", \"full_table_name\": \"dataverse.od_donation\"}'

# Conditional schema management activities
table_creation_result = '{\"dataRead\":476,\"dataWritten\":15629,\"filesWritten\":1,\"sourcePeakConnections\":1,\"sinkPeakConnections\":1,\"rowsRead\":1,\"rowsCopied\":1,\"copyDuration\":33,\"throughput\":0.025,\"errors\":[],\"effectiveIntegrationRuntime\":\"WorkspaceIR (West Europe)\",\"usedDataIntegrationUnits\":4,\"billingReference\":{\"activityType\":\"DataMovement\",\"billableDuration\":[{\"meterType\":\"AzureIR\",\"duration\":0.06666666666666667,\"unit\":\"DIUHours\"}],\"totalBillableDuration\":[{\"meterType\":\"AzureIR\",\"duration\":0.06666666666666667,\"unit\":\"DIUHours\"}]},\"usedParallelCopies\":1,\"executionDetails\":[{\"source\":{\"type\":\"CommonDataServiceForApps\"},\"sink\":{\"type\":\"Lakehouse\"},\"status\":\"Succeeded\",\"start\":\"2025-09-16T15:13:39.8847798Z\",\"duration\":33,\"usedDataIntegrationUnits\":4,\"usedParallelCopies\":1,\"profile\":{\"queue\":{\"status\":\"Completed\",\"duration\":12},\"transfer\":{\"status\":\"Completed\",\"duration\":19,\"details\":{\"readingFromSource\":{\"type\":\"CommonDataServiceForApps\",\"workingDuration\":0,\"timeToFirstByte\":0},\"writingToSink\":{\"type\":\"Lakehouse\",\"workingDuration\":0}}}},\"detailedDurations\":{\"queuingDuration\":12,\"timeToFirstByte\":0,\"transferDuration\":19}}],\"dataConsistencyVerification\":{\"VerificationResult\":\"NotVerified\"},\"durationInQueue\":{\"integrationRuntimeQueue\":0}}'
tracking_columns_result = '{\"status\":\"Succeeded\",\"result\":{\"runId\":\"2d48ef5a-7dcb-4a47-b34f-aad50b096833\",\"runStatus\":\"Succeeded\",\"sessionId\":\"e60296a9-70d5-4831-b3c4-f0c5460da7ad\",\"sparkPool\":\"b0f83c07-a701-49bb-a165-e06ca0ee4000\",\"error\":null,\"lastCheckedOn\":\"2025-09-16T15:16:17.1333333Z\",\"metadata\":{\"isForPipeline\":null,\"runStartTime\":\"2025-09-16T15:15:46.501302Z\",\"runEndTime\":\"2025-09-16T15:16:15.7981928Z\"},\"highConcurrencyModeStatus\":null,\"exitValue\":\"{\\\"status\\\": \\\"success\\\", \\\"table_name\\\": \\\"od_donation\\\", \\\"action\\\": \\\"tracking_columns_added\\\", \\\"columns_attempted\\\": 5, \\\"columns_added\\\": 5, \\\"columns_already_existed\\\": 0, \\\"message\\\": \\\"Added 5 tracking columns successfully\\\", \\\"execution_time_seconds\\\": 13.24, \\\"current_column_count\\\": 47, \\\"columns_added_list\\\": [\\\"IsDeleted\\\", \\\"IsPurged\\\", \\\"DeletedDate\\\", \\\"PurgedDate\\\", \\\"LastSynced\\\"], \\\"columns_existing_list\\\": [], \\\"error_message\\\": \\\"\\\", \\\"error_code\\\": \\\"\\\"}\"},\"message\":\"Notebook execution is in Succeeded state, runId: 2d48ef5a-7dcb-4a47-b34f-aad50b096833\",\"SparkMonitoringURL\":\"workloads/de-ds/sparkmonitor/271e1514-ff52-43e9-a131-33362fefae3c/e60296a9-70d5-4831-b3c4-f0c5460da7ad\",\"sessionSource\":\"\",\"sessionTag\":\"\",\"effectiveIntegrationRuntime\":\"AutoResolveIntegrationRuntime (West Europe)\",\"executionDuration\":34,\"durationInQueue\":{\"integrationRuntimeQueue\":0},\"billingReference\":{\"activityType\":\"ExternalActivity\",\"billableDuration\":[{\"meterType\":\"AzureIR\",\"duration\":0.016666666666666666,\"unit\":\"Hours\"}]}}'

# Conditional sync operation activities (Multiple inputs)
upsert_records_result = ''
append_records_result = '{\"dataRead\":914578,\"dataWritten\":107019,\"filesWritten\":1,\"sourcePeakConnections\":1,\"sinkPeakConnections\":1,\"rowsRead\":2027,\"rowsCopied\":2027,\"copyDuration\":26,\"throughput\":83.143,\"errors\":[],\"effectiveIntegrationRuntime\":\"WorkspaceIR (West Europe)\",\"usedDataIntegrationUnits\":4,\"billingReference\":{\"activityType\":\"DataMovement\",\"billableDuration\":[{\"meterType\":\"AzureIR\",\"duration\":0.06666666666666667,\"unit\":\"DIUHours\"}],\"totalBillableDuration\":[{\"meterType\":\"AzureIR\",\"duration\":0.06666666666666667,\"unit\":\"DIUHours\"}]},\"usedParallelCopies\":1,\"executionDetails\":[{\"source\":{\"type\":\"CommonDataServiceForApps\"},\"sink\":{\"type\":\"Lakehouse\"},\"status\":\"Succeeded\",\"start\":\"2025-09-16T15:18:12.545672Z\",\"duration\":26,\"usedDataIntegrationUnits\":4,\"usedParallelCopies\":1,\"profile\":{\"queue\":{\"status\":\"Completed\",\"duration\":10},\"transfer\":{\"status\":\"Completed\",\"duration\":15,\"details\":{\"readingFromSource\":{\"type\":\"CommonDataServiceForApps\",\"workingDuration\":0,\"timeToFirstByte\":4},\"writingToSink\":{\"type\":\"Lakehouse\",\"workingDuration\":0}}}},\"detailedDurations\":{\"queuingDuration\":10,\"timeToFirstByte\":4,\"transferDuration\":11}}],\"dataConsistencyVerification\":{\"VerificationResult\":\"NotVerified\"},\"durationInQueue\":{\"integrationRuntimeQueue\":0}}'
delete_detection_result = ''
purge_detection_result = ''
########################################

full_table_name = f"{schema_name}.{table_name}"

# Initialize results tracking
execution_results = {
    "status": "success",
    "full_table_name": full_table_name,
    "logs_written": {},
    "errors": []
}

# Check critical parameters
critical_params = {
    "full_table_name": full_table_name,
    "pipeline_run_id": pipeline_run_id, 
    "pipeline_trigger_time": pipeline_trigger_time,
    "schema_check_result": schema_check_result
}

parameter_issues = []
for param_name, param_value in critical_params.items():
    if "PARAM_NOT_SET" in str(param_value):
        parameter_issues.append(f"{param_name} not properly set")

if parameter_issues:
    execution_results["errors"].extend(parameter_issues)
    execution_results["status"] = "parameter_error"

print(f"Starting execution logging for table: {full_table_name}")
print(f"Pipeline Run ID: {pipeline_run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parse individual activity results
# Initialize all variables first to avoid NameError
schema_check = {}
table_creation = {}
tracking_columns = {}
upsert_activity = {}
append_activity = {}
delete_detection = {}
purge_detection = {}

# Smart JSON parsing with empty string handling
json_params = {
    'schema_check_result': (schema_check_result, schema_check),
    'table_creation_result': (table_creation_result, table_creation),
    'tracking_columns_result': (tracking_columns_result, tracking_columns),
    'upsert_records_result': (upsert_records_result, upsert_activity),
    'append_records_result': (append_records_result, append_activity),
    'delete_detection_result': (delete_detection_result, delete_detection),
    'purge_detection_result': (purge_detection_result, purge_detection)
}

for param_name, (param_value, target_dict) in json_params.items():
    # Skip null, empty strings, and PARAM_NOT_SET values
    if param_value not in ['null', '""', '', 'PARAM_NOT_SET'] and not param_value.startswith('PARAM_NOT_SET'):
        try:
            parsed_data = json.loads(param_value)
            target_dict.update(parsed_data)
        except json.JSONDecodeError as e:
            execution_results["errors"].append(f"JSON error in {param_name}: {str(e)}")
            print(f"JSON error in {param_name} at char {e.pos}: {str(e)}")

print(f"Parsed activity results:")
print(f"- Schema check: {'success' if not schema_check.get('error_message') else 'error'}")
print(f"- Table creation: {'executed' if table_creation else 'not_executed'}")
print(f"- Tracking columns: {'executed' if tracking_columns else 'not_executed'}")
print(f"- Upsert records: {'executed' if upsert_activity else 'not_executed'}")
print(f"- Append records: {'executed' if append_activity else 'not_executed'}")
print(f"- Delete detection: {'executed' if delete_detection else 'not_executed'}")
print(f"- Purge detection: {'executed' if purge_detection else 'not_executed'}")

# [Rest of the cell remains exactly the same]
def parse_copy_activity_success(activity_result):
    if not activity_result:
        return False, 0
    try:
        execution_details = activity_result.get('executionDetails', [])
        success = (
            execution_details and 
            execution_details[0].get('status') == 'Succeeded' and 
            activity_result.get('rowsCopied', 0) > 0 and
            len(activity_result.get('errors', [])) == 0
        )
        return success, activity_result.get('rowsCopied', 0)
    except Exception as e:
        print(f"Error parsing copy activity: {e}")
        return False, 0

def parse_notebook_success_and_count(activity_result, count_field):
    if not activity_result:
        return False, 0
    try:
        if activity_result.get('result', {}).get('runStatus') != 'Succeeded':
            return False, 0
        exit_value_str = activity_result.get('result', {}).get('exitValue', '{}')
        exit_value = json.loads(exit_value_str)
        return exit_value.get('status') == 'success', exit_value.get(count_field, 0)
    except Exception as e:
        print(f"Error parsing notebook activity: {e}")
        return False, 0

# Determine primary sync activity and build synthesized result
append_success, append_count = parse_copy_activity_success(append_activity)
upsert_success, upsert_count = parse_copy_activity_success(upsert_activity)
delete_success, delete_count = parse_notebook_success_and_count(delete_detection, 'deleted_records')
purge_success, purge_count = parse_notebook_success_and_count(purge_detection, 'purged_records')

print(f"Activity parsing results:")
print(f"- Append: success={append_success}, count={append_count}")
print(f"- Upsert: success={upsert_success}, count={upsert_count}")
print(f"- Delete: success={delete_success}, count={delete_count}")
print(f"- Purge: success={purge_success}, count={purge_count}")

# Build synthesized sync_operations_result
if append_success:
    sync_operations = {
        "status": "success",
        "total_records": append_count,
        "sync_type": "initial",
        "deleted_records": delete_count,
        "purged_records": purge_count
    }
elif upsert_success:
    sync_operations = {
        "status": "success", 
        "total_records": upsert_count,
        "sync_type": "daily",
        "deleted_records": delete_count,
        "purged_records": purge_count
    }
else:
    sync_operations = {
        "status": "error",
        "total_records": 0,
        "sync_type": "failed",
        "deleted_records": 0,
        "purged_records": 0
    }

print(f"Synthesized sync operations: {sync_operations}")

# Generate timestamps - simplified approach with timezone consistency
try:
    # Always use current time to avoid parsing issues
    from datetime import timezone
    end_time = datetime.now(timezone.utc)  # Make timezone-aware
    
    if not pipeline_trigger_time.startswith('PARAM_NOT_SET'):
        # Try to parse, but fall back gracefully
        try:
            timestamp_str = pipeline_trigger_time.replace('Z', '+00:00')
            # Remove extra microsecond digits if present
            import re
            timestamp_str = re.sub(r'\.(\d{6})\d*', r'.\1', timestamp_str)
            start_time = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f%z')
            print(f"Parsed trigger time: {start_time}")
        except Exception as parse_error:
            print(f"Failed to parse trigger time: {parse_error}, using current time")
            start_time = end_time  # Use current time if parsing fails
    else:
        start_time = end_time
        execution_results["errors"].append("pipeline_trigger_time parameter not set")
    
    print(f"Execution window: {start_time} to {end_time}")
    
except Exception as e:
    execution_results["errors"].append(f"Error generating timestamps: {str(e)}")
    print(f"Error generating timestamps: {e}")
    # Ensure both are timezone-aware
    from datetime import timezone
    start_time = end_time = datetime.now(timezone.utc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define schema and prepare safe values
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
    StructField("RetryCount", IntegerType(), False),
    StructField("CreatedDate", TimestampType(), False)
])
safe_pipeline_run_id = pipeline_run_id if not pipeline_run_id.startswith('PARAM_NOT_SET') else 'UNKNOWN_RUN_ID'
safe_table_name = table_name if not table_name.startswith('PARAM_NOT_SET') else 'UNKNOWN_TABLE'
safe_schema_name = schema_name if not schema_name.startswith('PARAM_NOT_SET') else 'UNKNOWN_SCHEMA'
safe_full_table_name = f"{safe_schema_name}.{safe_table_name}"
log_entries = []
# 1. Schema Check entry (always executed)
schema_error_msg = schema_check.get('error_message') if schema_check.get('error_message') else None
schema_status = 'Error' if schema_error_msg else 'Success'
log_entries.append((
    str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'SchemaCheck',
    start_time, end_time, 0, 0, 0, schema_status, schema_error_msg, 0, end_time
))
# 2. Table Creation entries (conditional)
if table_creation:
    table_creation_succeeded, rows_copied = parse_copy_activity_success(table_creation)
    if table_creation_succeeded:
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'CreateTable',
            start_time, end_time, rows_copied, 0, 0, 'Success', None, 0, end_time
        ))
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'TruncateTable',
            start_time, end_time, 0, 1, 0, 'Success', None, 0, end_time
        ))
# 3. Tracking Columns entry (conditional)
if tracking_columns:
    if tracking_columns.get('result', {}).get('runStatus') == 'Succeeded':
        try:
            exit_value = json.loads(tracking_columns.get('result', {}).get('exitValue', '{}'))
            if exit_value.get('status') == 'success' and exit_value.get('columns_added', 0) > 0:
                log_entries.append((
                    str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'AddTrackingColumns',
                    start_time, end_time, 0, 0, 0, 'Success', None, 0, end_time
                ))
        except json.JSONDecodeError:
            pass
# 4. Sync Operations entries
if append_success:
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'InitialSync',
        start_time, end_time, append_count, delete_count, purge_count, 'Success', None, 0, end_time
    ))
elif upsert_success:
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'DataSync',
        start_time, end_time, upsert_count, delete_count, purge_count, 'Success', None, 0, end_time
    ))
# 5. Parameter error entry if needed
if execution_results["errors"]:
    error_message = f"Parameter errors: {'; '.join(execution_results['errors'])}"
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'ParameterValidation',
        start_time, end_time, 0, 0, 0, 'Error', error_message, 0, end_time
    ))
# Insert log entries
if log_entries:
    sync_audit_df = spark.createDataFrame(log_entries, sync_audit_schema)
    sync_audit_df.write.format("delta").mode("append").saveAsTable("metadata.SyncAuditLog")
    execution_results["logs_written"]["sync_audit_log"] = len(log_entries)
    print(f"Inserted {len(log_entries)} entries into metadata.SyncAuditLog")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DataValidation entry - proper validation logic
if sync_operations["status"] == "success":
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
        FROM {safe_full_table_name}
        """
        
        bronze_counts = spark.sql(bronze_counts_query).collect()[0]
        
        total_records = bronze_counts['total_records']
        active_records = bronze_counts['active_records'] 
        deleted_records = bronze_counts['deleted_records']
        purged_records = bronze_counts['purged_records']
        
        # Determine expected source count based on sync type
        if sync_operations["sync_type"] == "initial":
            # For initial sync: source should match what we copied (all active records in Dataverse)
            expected_source_count = sync_operations["total_records"]
        else:
            # For daily sync: source count should match current active records in Bronze
            # (assuming Dataverse active count matches Bronze active count)
            expected_source_count = active_records
        
        # Validation checks
        validation_errors = []
        
        # 1. Internal consistency check: total = active + deleted + purged
        calculated_total = active_records + deleted_records + purged_records
        if total_records != calculated_total:
            validation_errors.append(f"Total mismatch: {total_records} != {calculated_total}")
        
        # 2. For initial sync: validate we got all the records we expected
        if sync_operations["sync_type"] == "initial":
            if active_records != sync_operations["total_records"]:
                validation_errors.append(f"Initial sync mismatch: active={active_records}, copied={sync_operations['total_records']}")
        
        # 3. Check for reasonable delete/purge counts
        if deleted_records < 0 or purged_records < 0:
            validation_errors.append(f"Negative counts: deleted={deleted_records}, purged={purged_records}")
        
        # 4. Check sync operation consistency with detected counts
        sync_deleted = sync_operations.get("deleted_records", 0)
        sync_purged = sync_operations.get("purged_records", 0)
        
        # Note: These might not match exactly due to timing, but huge discrepancies indicate issues
        delete_discrepancy = abs(deleted_records - sync_deleted)
        purge_discrepancy = abs(purged_records - sync_purged) 
        
        if delete_discrepancy > max(10, deleted_records * 0.1):  # 10 records or 10% tolerance
            validation_errors.append(f"Delete count discrepancy: bronze={deleted_records}, sync={sync_deleted}")
            
        if purge_discrepancy > max(10, purged_records * 0.1):  # 10 records or 10% tolerance  
            validation_errors.append(f"Purge count discrepancy: bronze={purged_records}, sync={sync_purged}")
        
        # Overall validation result
        validation_passed = len(validation_errors) == 0
        
        if validation_errors:
            print(f"Validation errors for {safe_full_table_name}: {validation_errors}")
            execution_results["errors"].extend(validation_errors)
        
        validation_entry = [(
            str(uuid.uuid4()), 
            end_time, 
            safe_full_table_name, 
            expected_source_count,  # Best estimate of Dataverse count
            total_records,          # Actual Bronze total
            active_records,         # Actual Bronze active
            deleted_records,        # Actual Bronze deleted
            purged_records,         # Actual Bronze purged
            validation_passed       # True only if all validations pass
        )]
        
        validation_df = spark.createDataFrame(validation_entry, validation_schema)
        validation_df.write.format("delta").mode("append").saveAsTable("metadata.DataValidation")
        execution_results["logs_written"]["data_validation"] = 1
        
        print(f"DataValidation: source={expected_source_count}, total={total_records}, active={active_records}, deleted={deleted_records}, purged={purged_records}, passed={validation_passed}")
        
    except Exception as e:
        # If validation query fails, log the error but continue
        execution_results["errors"].append(f"DataValidation query failed: {str(e)}")
        
        # Insert a failed validation record
        validation_entry = [(
            str(uuid.uuid4()), end_time, safe_full_table_name, 
            None, 0, 0, 0, 0, False  # All counts zero, validation failed
        )]
        
        validation_df = spark.createDataFrame(validation_entry, validation_schema)
        validation_df.write.format("delta").mode("append").saveAsTable("metadata.DataValidation")
        execution_results["logs_written"]["data_validation"] = 1
        
        print(f"DataValidation failed due to query error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update LastDailySync only for successfully synced tables
if sync_operations.get('status') == 'success' and not table_name.startswith('PARAM_NOT_SET'):
    try:
        spark.sql(f"""
            UPDATE metadata.PipelineConfig 
            SET LastDailySync = '{end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' 
            WHERE TableName = '{table_name}'
        """)
        
        execution_results["logs_written"]["pipeline_config_update"] = 1
        print(f"Updated metadata.PipelineConfig.LastDailySync for {table_name}")
        
    except Exception as e:
        execution_results["errors"].append(f"Error updating PipelineConfig: {str(e)}")
        print(f"Error updating PipelineConfig: {e}")
elif table_name.startswith('PARAM_NOT_SET'):
    execution_results["errors"].append("Cannot update PipelineConfig: table_name parameter not set")
    print(f"Skipping PipelineConfig update: table_name parameter not set")
else:
    print(f"Skipping PipelineConfig update for {table_name} (sync not successful)")
# Create Checkpoint Entry (simplified - only for successful operations)
create_checkpoint = sync_operations.get('status') == 'success' and not any(err for err in execution_results["errors"] if "PARAM_NOT_SET" in err)
if create_checkpoint:
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
    
    from datetime import timedelta
    retention_date = (end_time + timedelta(days=7)).date()
    
    checkpoint_entry = [(
        str(uuid.uuid4()),
        f"bronze_backup_{end_time.strftime('%Y-%m-%d')}",
        'Daily',
        end_time,
        1,  # This table (in real implementation, count all successful tables)
        sync_operations.get('total_records', 0),
        'Validated',
        retention_date,
        True
    )]
    
    checkpoint_df = spark.createDataFrame(checkpoint_entry, checkpoint_schema)
    
    checkpoint_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("metadata.CheckpointHistory")
    
    execution_results["logs_written"]["checkpoint_history"] = 1
    print(f"Created checkpoint entry: bronze_backup_{end_time.strftime('%Y-%m-%d')}")
else:
    print("No checkpoint entry created (sync not successful, parameter errors, or not applicable)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate execution summary
execution_summary = {
    "actions_taken": [],
    "execution_time_ms": int((end_time - start_time).total_seconds() * 1000),
    "logs_written": execution_results["logs_written"],
    "sync_operations": sync_operations
}

# Determine actions taken
if table_creation and parse_copy_activity_success(table_creation)[0]:
    execution_summary["actions_taken"].append("table_created")

if tracking_columns:
    try:
        exit_value = json.loads(tracking_columns.get('result', {}).get('exitValue', '{}'))
        if exit_value.get('status') == 'success' and exit_value.get('columns_added', 0) > 0:
            execution_summary["actions_taken"].append("tracking_columns_added")
    except:
        pass

if sync_operations["status"] == "success":
    if sync_operations["sync_type"] == "initial":
        execution_summary["actions_taken"].append("initial_sync_completed")
    else:
        execution_summary["actions_taken"].append("data_synced")

# Update final results
execution_results.update({
    "execution_summary": execution_summary,
    "total_logs_written": sum(execution_results["logs_written"].values())
})

if parameter_issues:
    execution_results["status"] = "parameter_error"

# Print final summary
print(f"\n=== Execution Logging Complete ===")
print(f"Table: {full_table_name}")
print(f"Status: {execution_results['status']}")
print(f"Actions taken: {execution_summary['actions_taken']}")
print(f"Sync type: {sync_operations['sync_type']}")
print(f"Records processed: {sync_operations['total_records']}")
print(f"Total log entries written: {execution_results['total_logs_written']}")

if execution_results["errors"]:
    print(f"Errors: {execution_results['errors']}")

# Return results
mssparkutils.notebook.exit(json.dumps(execution_results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
