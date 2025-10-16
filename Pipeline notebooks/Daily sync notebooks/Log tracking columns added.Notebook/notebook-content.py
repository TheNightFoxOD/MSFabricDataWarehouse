# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4aee8a32-be91-489f-89f3-1a819b188807",
# META       "default_lakehouse_name": "Bronze",
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

# Relevant activity results for schema management
tracking_columns_result = 'PARAM_NOT_SET_tracking_columns_result'

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
    "tracking_columns_result": tracking_columns_result
}

for param_name, param_value in critical_params.items():
    if "PARAM_NOT_SET" in str(param_value):
        execution_results["errors"].append(f"{param_name} not properly set")

print(f"Schema Management Logging for table: {full_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parse tracking columns result
tracking_columns = {}

if tracking_columns_result not in ['null', '""', '', 'PARAM_NOT_SET'] and not tracking_columns_result.startswith('PARAM_NOT_SET'):
    try:
        tracking_columns = json.loads(tracking_columns_result)
    except json.JSONDecodeError as e:
        execution_results["errors"].append(f"JSON error in tracking_columns_result: {str(e)}")
        print(f"JSON error in tracking_columns_result: {str(e)}")

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

print(f"Tracking columns: {'executed' if tracking_columns else 'not_executed'}")

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
    StructField("RetryCount", IntegerType(), False),
    StructField("CreatedDate", TimestampType(), False)
])

safe_pipeline_run_id = pipeline_run_id if not pipeline_run_id.startswith('PARAM_NOT_SET') else 'UNKNOWN_RUN_ID'
safe_full_table_name = full_table_name if not "PARAM_NOT_SET" in full_table_name else 'UNKNOWN_TABLE'

log_entries = []

# Add Tracking Columns entry - Fixed logic for direct exitValue input
if tracking_columns and tracking_columns.get('status') == 'success' and tracking_columns.get('columns_added', 0) > 0:
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'AddTrackingColumns',
        start_time, end_time, 0, 0, 0, 'Success', None, 0, end_time
    ))
    print(f"Added tracking columns log entry for {safe_full_table_name}")
elif tracking_columns and tracking_columns.get('status') == 'success' and tracking_columns.get('columns_added', 0) == 0:
    # Columns already existed scenario
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'AddTrackingColumns',
        start_time, end_time, 0, 0, 0, 'Skipped', 'No columns needed to be added', 0, end_time
    ))
    print(f"Added tracking columns skipped entry for {safe_full_table_name}")
elif tracking_columns:
    # Error scenario
    error_msg = tracking_columns.get('error_message', 'Unknown error in tracking columns operation')
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'AddTrackingColumns',
        start_time, end_time, 0, 0, 0, 'Error', error_msg, 0, end_time
    ))
    print(f"Added tracking columns error entry for {safe_full_table_name}")

# Insert log entries
if log_entries:
    sync_audit_df = spark.createDataFrame(log_entries, sync_audit_schema)
    sync_audit_df.write.format("delta").mode("append").saveAsTable("metadata.SyncAuditLog")
    execution_results["logs_written"]["sync_audit_log"] = len(log_entries)
    print(f"Inserted {len(log_entries)} entries into metadata.SyncAuditLog")
else:
    print("No log entries to insert - tracking_columns parsing may have failed")
    print(f"tracking_columns content: {tracking_columns}")

# Return results
execution_results["total_logs_written"] = sum(execution_results["logs_written"].values())
print(f"Schema management logging complete: {execution_results}")
mssparkutils.notebook.exit(json.dumps(execution_results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
