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
# META         },
# META         {
# META           "id": "234e6789-2254-455f-b2b2-36d881cb1c17"
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
staging_lakehouse = "default_staging_lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL
# table_name = "account"
# schema_name = "dataverse"
# pipeline_run_id = "db506216-0b3b-4a34-b414-810480fdf313"
# pipeline_trigger_time = "2024-01-01T00:00:00Z"

# CELL 1 - Main Schema Drift Logic
import json
import uuid
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Initialize results
results = {
    "status": "success",
    "operation": "SchemaDrift",
    "columns_added": 0,
    "columns_dropped": 0,
    "columns_added_list": [],
    "columns_dropped_list": [],
    "columns_unchanged": 0,
    "action": "schema_drift_handled",
    "message": "",
    "error_message": ""
}

execution_start = datetime.now()
safe_table_name = table_name.replace("'", "''")
safe_schema_name = schema_name.replace("'", "''")
full_table_name = f"{safe_schema_name}.{safe_table_name}"
staging_table_name = f"{staging_lakehouse}.dbo.{safe_table_name}"
safe_pipeline_run_id = pipeline_run_id.replace("'", "''")

try:
    print(f"Starting schema drift detection for {full_table_name}")
    print(f"Pipeline Run ID: {safe_pipeline_run_id}")
    print(f"Comparing with staging: {staging_table_name}")
    
    # Step 1: Verify target Bronze table exists
    print(f"\n--- Step 1: Verify Bronze table existence ---")
    bronze_tables = spark.sql(f"SHOW TABLES IN {safe_schema_name}").collect()
    bronze_table_exists = any(row['tableName'].lower() == safe_table_name.lower() for row in bronze_tables)
    
    if not bronze_table_exists:
        results["status"] = "error"
        results["error_message"] = f"Bronze table {full_table_name} does not exist"
        print(f"ERROR: {results['error_message']}")
        raise Exception(results["error_message"])
    
    print(f"✓ Bronze table exists: {full_table_name}")
    
    # Step 2: Verify staging table exists (source of truth for current schema)
    print(f"\n--- Step 2: Verify staging table existence ---")
    staging_tables = spark.sql(f"SHOW TABLES IN Dataverse_Master_Staging.dbo").collect()
    staging_table_exists = any(row['tableName'].lower() == safe_table_name.lower() for row in staging_tables)
    
    if not staging_table_exists:
        results["status"] = "warning"
        results["message"] = f"Staging table {staging_table_name} not found - schema drift check skipped"
        print(f"WARNING: {results['message']}")
        print("This table may not be configured for Link to Fabric sync")
        mssparkutils.notebook.exit(json.dumps(results))
    
    print(f"✓ Staging table exists: {staging_table_name}")
    
    # Step 3: Get Bronze layer schema (current target schema)
    print(f"\n--- Step 3: Get Bronze layer current schema ---")
    bronze_schema_raw = spark.sql(f"DESCRIBE {full_table_name}").collect()
    
    # Filter out metadata rows and tracking columns
    tracking_columns_lower = ['isdeleted', 'ispurged', 'deleteddate', 'purgeddate', 'lastsynced', 'syncdate', 'syncoperation']
    bronze_columns = {}
    
    for row in bronze_schema_raw:
        col_name = row['col_name']
        col_type = row['data_type']
        
        # Skip metadata rows, rows starting with #, and tracking columns
        if (not col_name or col_name.startswith('#') or not col_type or 
            col_name.lower() in tracking_columns_lower):
            continue
        
        bronze_columns[col_name.lower()] = {
            'name': col_name,
            'type': col_type
        }
    
    print(f"Bronze layer has {len(bronze_columns)} business columns (excluding tracking columns)")
    
    # Step 4: Get staging table schema (source schema from Dataverse via Link to Fabric)
    print(f"\n--- Step 4: Get staging table schema (source of truth) ---")
    staging_schema_raw = spark.sql(f"DESCRIBE {staging_table_name}").collect()
    
    # Link to Fabric adds system columns
    # We EXCLUDE from comparison: Id, IsDelete, PartitionId (dropped from Bronze)
    # We INCLUDE in comparison: SinkCreatedOn, SinkModifiedOn (kept in Bronze)
    fabric_system_columns_to_exclude = ['id', 'isdelete', 'partitionid']
    
    staging_columns = {}
    excluded_count = 0
    for row in staging_schema_raw:
        col_name = row['col_name']
        col_type = row['data_type']
        col_lower = col_name.lower() if col_name else ''
        
        # Skip metadata rows and rows starting with #
        if not col_name or col_name.startswith('#') or not col_type:
            continue
        
        # Exclude only the 3 Fabric columns we drop (Id, IsDelete, PartitionId)
        if col_lower in fabric_system_columns_to_exclude:
            excluded_count += 1
            print(f"  Excluding Fabric system column: {col_name}")
            continue
        
        staging_columns[col_lower] = {
            'name': col_name,
            'type': col_type
        }
    
    print(f"Staging table has {len(staging_columns)} Dataverse business columns")
    print(f"  (Excluded {excluded_count} Fabric system columns: Id, IsDelete, PartitionId)")
    print(f"  (Included SinkCreatedOn, SinkModifiedOn as business columns)")
    
    # Step 5: Detect schema drift
    print(f"\n--- Step 5: Detect schema drift ---")
    
    # Columns in staging but not in Bronze = ADDITIONS
    added_columns = []
    for col_lower, col_info in staging_columns.items():
        if col_lower not in bronze_columns:
            added_columns.append(col_info)
            print(f"+ Column ADDED in source: {col_info['name']} ({col_info['type']})")
    
    # Columns in Bronze but not in staging = DROPS (but we keep them)
    dropped_columns = []
    for col_lower, col_info in bronze_columns.items():
        if col_lower not in staging_columns:
            dropped_columns.append(col_info)
            print(f"- Column DROPPED in source (keeping in Bronze): {col_info['name']} ({col_info['type']})")
    
    # Unchanged columns
    unchanged_count = len([c for c in bronze_columns.keys() if c in staging_columns])
    
    results["columns_added"] = len(added_columns)
    results["columns_dropped"] = len(dropped_columns)
    results["columns_unchanged"] = unchanged_count
    results["columns_added_list"] = [col['name'] for col in added_columns]
    results["columns_dropped_list"] = [col['name'] for col in dropped_columns]
    
    print(f"\nSchema Drift Summary:")
    print(f"  - Columns to ADD: {results['columns_added']}")
    print(f"  - Columns DROPPED (kept): {results['columns_dropped']}")
    print(f"  - Columns unchanged: {results['columns_unchanged']}")
    
    # Step 6: Handle column additions
    if added_columns:
        print(f"\n--- Step 6: Adding new columns to Bronze layer ---")
        
        # Build ALTER TABLE statement
        columns_to_add = []
        for col in added_columns:
            # Note: We don't use DEFAULT values to match existing pattern
            columns_to_add.append(f"{col['name']} {col['type']}")
        
        columns_clause = ", ".join(columns_to_add)
        alter_sql = f"ALTER TABLE {full_table_name} ADD COLUMNS ({columns_clause})"
        
        print(f"Executing: {alter_sql}")
        spark.sql(alter_sql)
        
        print(f"✓ Successfully added {len(added_columns)} columns to {full_table_name}")
        results["action"] = "columns_added"
        results["message"] = f"Added {len(added_columns)} new columns"
    else:
        print(f"\n--- Step 6: No columns to add ---")
        results["action"] = "no_additions"
        results["message"] = "No new columns detected"
    
    # Step 7: Log column drops (informational only - we keep the columns)
    if dropped_columns:
        print(f"\n--- Step 7: Logging dropped columns (columns kept in Bronze) ---")
        print(f"Note: {len(dropped_columns)} columns were dropped from source but retained in Bronze")
        
        if not added_columns:
            results["action"] = "columns_dropped_logged"
            results["message"] = f"Logged {len(dropped_columns)} dropped columns (kept in Bronze)"
    
    execution_end = datetime.now()
    
    # Step 8: Write audit logs to metadata.SyncAuditLog
    print(f"\n--- Step 8: Writing audit logs ---")
    
    log_entries = []
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
    
    # Log column additions
    if added_columns:
        col_names = ', '.join([col['name'] for col in added_columns])
        note_msg = f"Columns added: {col_names}"[:1000]
        log_entries.append((
            str(uuid.uuid4()),
            safe_pipeline_run_id,
            'DailySync',
            full_table_name,
            'ColumnAddition',
            execution_start,
            execution_end,
            len(added_columns),
            0,
            0,
            'Success',
            None,
            note_msg,
            0,
            execution_end
        ))
        print(f"+ Added ColumnAddition audit log entry")
    
    # Log column drops (informational)
    if dropped_columns:
        col_names = ', '.join([col['name'] for col in dropped_columns])
        note_msg = f"Columns dropped in source (kept in Bronze): {col_names}"[:1000]
        log_entries.append((
            str(uuid.uuid4()),
            safe_pipeline_run_id,
            'DailySync',
            full_table_name,
            'ColumnDrop',
            execution_start,
            execution_end,
            len(dropped_columns),
            0,
            0,
            'Success',
            None,
            note_msg,
            0,
            execution_end
        ))
        print(f"+ Added ColumnDrop audit log entry")
    
    # Insert audit log entries
    if log_entries:
        sync_audit_df = spark.createDataFrame(log_entries, sync_audit_schema)
        sync_audit_df.write.format("delta").mode("append").saveAsTable("metadata.SyncAuditLog")
        print(f"✓ Inserted {len(log_entries)} audit log entries")
    else:
        print("No audit log entries to insert (no schema changes detected)")
    
    print(f"\n{'='*60}")
    print(f"Schema drift handling completed successfully")
    print(f"{'='*60}")
    
except Exception as e:
    execution_end = datetime.now()
    results["status"] = "error"
    results["error_message"] = str(e)
    print(f"\nERROR during schema drift handling: {e}")
    
    # Log the error to SyncAuditLog
    try:
        error_log = [(
            str(uuid.uuid4()),
            safe_pipeline_run_id,
            'DailySync',
            full_table_name,
            'SchemaDrift',
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
print(f"\nReturning results: {results}")
mssparkutils.notebook.exit(json.dumps(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
