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

# Relevant activity results for daily sync
upsert_records_result = 'PARAM_NOT_SET_upsert_records_result'  # Merge staged result
delete_detection_result = 'PARAM_NOT_SET_delete_detection_result'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import uuid
from datetime import datetime, timezone
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

# Check critical parameters
critical_params = {
    "full_table_name": full_table_name,
    "pipeline_run_id": pipeline_run_id,
    "upsert_records_result": upsert_records_result,
    "delete_detection_result": delete_detection_result
}

for param_name, param_value in critical_params.items():
    if "PARAM_NOT_SET" in str(param_value):
        execution_results["errors"].append(f"{param_name} not properly set")

print(f"Daily Sync Logging for table: {full_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parse activity results
upsert_activity = {}
delete_detection = {}

json_params = {
    'upsert_records_result': (upsert_records_result, upsert_activity),
    'delete_detection_result': (delete_detection_result, delete_detection)
}

for param_name, (param_value, target_dict) in json_params.items():
    if param_value not in ['null', '""', '', 'PARAM_NOT_SET'] and not param_value.startswith('PARAM_NOT_SET'):
        try:
            parsed_data = json.loads(param_value)
            target_dict.update(parsed_data)
        except json.JSONDecodeError as e:
            execution_results["errors"].append(f"JSON error in {param_name}: {str(e)}")
            print(f"JSON error in {param_name}: {str(e)}")

# Parse direct exitValue results (not full activity output)
def parse_exitvalue_success_and_count(exit_value_dict, count_field):
    if not exit_value_dict:
        return False, 0
    try:
        status_success = exit_value_dict.get('status') == 'success'
        count = exit_value_dict.get(count_field, 0)
        return status_success, count
    except Exception as e:
        print(f"Error parsing exitValue: {e}")
        return False, 0

# Parse results with correct count fields
upsert_success, upsert_count = parse_exitvalue_success_and_count(upsert_activity, 'upsert_count')  # Changed field name
delete_success, delete_count = parse_exitvalue_success_and_count(delete_detection, 'deleted_records')
_, purge_count = parse_exitvalue_success_and_count(delete_detection, 'purged_records')

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

print(f"Upsert: success={upsert_success}, count={upsert_count}")
print(f"Delete detection: success={delete_success}, deleted={delete_count}, purged={purge_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
    StructField("RetryCount", IntegerType(), False),
    StructField("CreatedDate", TimestampType(), False)
])

safe_pipeline_run_id = pipeline_run_id if not pipeline_run_id.startswith('PARAM_NOT_SET') else 'UNKNOWN_RUN_ID'
safe_full_table_name = full_table_name if not "PARAM_NOT_SET" in full_table_name else 'UNKNOWN_TABLE'

log_entries = []

# 1. Data Sync entry (from Merge staged) - Log both success and failure
if upsert_activity:  # Check if activity was attempted (has data)
    if upsert_success:
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'DataSync',
            start_time, end_time, upsert_count, 0, 0, 'Success', None, None, 0, end_time
        ))
        print(f"Logged successful DataSync: {upsert_count} records")
    else:
        # Activity attempted but failed - extract error message
        error_msg = upsert_activity.get('error_message', 'DataSync operation failed')
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'DataSync',
            start_time, end_time, 0, 0, 0, 'Error', error_msg, None, 0, end_time
        ))
        print(f"Logged failed DataSync: {error_msg}")

# 2. Delete Detection entry - Log both success and failure  
if delete_detection:  # Check if activity was attempted (has data)
    if delete_success:
        # Determine which detection mode was used
        # Check if purge logic was applied (purge_count > 0 OR purge_date configured)
        purge_date_value = delete_detection.get('purge_date')
        
        if purge_count > 0 or purge_date_value:
            # Purge-aware detection was applied
            if purge_date_value:
                note_msg = f"Purge-aware detection applied (cutoff: {purge_date_value})"
            else:
                note_msg = "Purge-aware detection applied (cutoff: configured date)"
        else:
            # Standard delete detection (no purge date configured)
            note_msg = "Standard delete detection applied"
        
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'DeleteDetection',
            start_time, end_time, 0, delete_count, purge_count, 'Success', None, note_msg, 0, end_time
        ))
        print(f"Added DeleteDetection log entry with notes: {note_msg}")
        print(f"  RowsDeleted={delete_count}, RowsPurged={purge_count}")
    elif delete_detection:
        # DeleteDetection attempted but failed
        error_msg = delete_detection.get('error_message', 'Delete detection failed')
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'DeleteDetection',
            start_time, end_time, 0, 0, 0, 'Error', error_msg, None, 0, end_time
        ))
        print(f"Added DeleteDetection error entry")

# 3. Handle case where no activities were attempted due to parameter issues
if not upsert_activity and not delete_detection:
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'DailySyncAttempt',
        start_time, end_time, 0, 0, 0, 'Error', 
        'No daily sync activities could be parsed from input parameters', 0, end_time
    ))
    print("Logged daily sync failure due to parameter parsing issues")

# Insert log entries - now always logs something
if log_entries:
    sync_audit_df = spark.createDataFrame(log_entries, sync_audit_schema)
    sync_audit_df.write.format("delta").mode("append").saveAsTable("metadata.SyncAuditLog")
    execution_results["logs_written"]["sync_audit_log"] = len(log_entries)
    print(f"Inserted {len(log_entries)} entries into metadata.SyncAuditLog")
else:
    print("WARNING: No log entries created - this should not happen")

# Return results
execution_results["total_logs_written"] = sum(execution_results["logs_written"].values())
print(f"Daily sync logging complete: {execution_results}")
mssparkutils.notebook.exit(json.dumps(execution_results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
