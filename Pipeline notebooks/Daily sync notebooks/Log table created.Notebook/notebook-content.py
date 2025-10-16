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

# Relevant activity results for initial sync
schema_check_result = 'PARAM_NOT_SET_schema_check_result'
table_creation_result = 'PARAM_NOT_SET_table_creation_result'

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
    "schema_check_result": schema_check_result,
    "table_creation_result": table_creation_result
}

for param_name, param_value in critical_params.items():
    if "PARAM_NOT_SET" in str(param_value):
        execution_results["errors"].append(f"{param_name} not properly set")

print(f"Initial Sync Logging for table: {full_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parse activity results
schema_check = {}
table_creation = {}

json_params = {
    'schema_check_result': (schema_check_result, schema_check),
    'table_creation_result': (table_creation_result, table_creation)
}

for param_name, (param_value, target_dict) in json_params.items():
    if param_value not in ['null', '""', '', 'PARAM_NOT_SET'] and not param_value.startswith('PARAM_NOT_SET'):
        try:
            parsed_data = json.loads(param_value)
            target_dict.update(parsed_data)
        except json.JSONDecodeError as e:
            execution_results["errors"].append(f"JSON error in {param_name}: {str(e)}")
            print(f"JSON error in {param_name}: {str(e)}")

# Parse copy activity success
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

table_creation_success, table_creation_count = parse_copy_activity_success(table_creation)

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

print(f"Table creation: success={table_creation_success}, count={table_creation_count}")

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

# 1. Schema Check entry - Log both success and failure
if schema_check:
    schema_error_msg = schema_check.get('error_message')
    if schema_error_msg:
        # Error scenario - no Notes, use ErrorMessage
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'SchemaCheck',
            start_time, end_time, 0, 0, 0, 'Error', schema_error_msg, None, 0, end_time
        ))
        print(f"Logged failed SchemaCheck: {schema_error_msg}")
    else:
        # Success - capture what schema check determined
        required_actions = schema_check.get('required_actions', [])
        if required_actions:
            note_msg = f"Required actions: {', '.join(required_actions)}"
        else:
            note_msg = "No actions required"
        
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'SchemaCheck',
            start_time, end_time, 0, 0, 0, 'Success', None, note_msg, 0, end_time
        ))
        print(f"Logged successful SchemaCheck with notes: {note_msg}")
else:
    # Schema check not attempted due to parameter issues
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'SchemaCheck',
        start_time, end_time, 0, 0, 0, 'Error', 
        'Schema check could not be parsed from input parameters', None, None, 0, end_time
    ))
    print("Logged schema check failure due to parameter parsing issues")

# 2. Create Table + Initial Sync entry - Log both success and failure
if table_creation:  # Check if activity was attempted (has data)
    if table_creation_success and table_creation_count > 0:
        # Log successful table creation
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'CreateTable',
            start_time, end_time, table_creation_count, 0, 0, 'Success', None, None, 0, end_time
        ))
        print(f"Logged successful CreateTable: {table_creation_count} records")
        
        # Log successful initial sync (same operation, different log entry)
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'InitialSync',
            start_time, end_time, table_creation_count, 0, 0, 'Success', None, None, 0, end_time
        ))
        print(f"Logged successful InitialSync: {table_creation_count} records")
    else:
        # Activity attempted but failed - extract error message
        error_msg = table_creation.get('errors', ['Table creation failed'])[0] if table_creation.get('errors') else 'Table creation operation failed'
        if isinstance(error_msg, dict):
            error_msg = error_msg.get('errorMessage', 'Table creation operation failed')
        
        log_entries.append((
            str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'CreateTable',
            start_time, end_time, 0, 0, 0, 'Error', str(error_msg), None, 0, end_time
        ))
        print(f"Logged failed CreateTable: {error_msg}")
else:
    # Table creation not attempted due to parameter issues
    log_entries.append((
        str(uuid.uuid4()), safe_pipeline_run_id, 'DailySync', safe_full_table_name, 'CreateTable',
        start_time, end_time, 0, 0, 0, 'Error', 
        'Table creation activity could not be parsed from input parameters', None, 0, end_time
    ))
    print("Logged table creation failure due to parameter parsing issues")

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
print(f"Initial sync logging complete: {execution_results}")
mssparkutils.notebook.exit(json.dumps(execution_results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
