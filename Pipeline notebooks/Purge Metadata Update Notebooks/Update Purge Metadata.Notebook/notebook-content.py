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

# Parameters cell - DO NOT modify this cell, values will be injected by pipeline
table_names = "table_names_default"
purge_date = "purge_date_default"
pipeline_run_id = "pipeline_run_id_default"
validation_result = "validation_result_default"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update Purge Metadata Notebook
# Purpose: Create checkpoint, update metadata, and log audit trail

import json
from datetime import datetime, timedelta
from pyspark.sql.functions import col, current_timestamp

# ==========================================
# STEP 1: Parse Input Parameters
# ==========================================
# Parameters are already defined in the parameters cell above
# The pipeline will inject actual values, replacing the defaults
validation_result_str = validation_result  # Rename for clarity in code

print(f"Update started for pipeline run: {pipeline_run_id}")
print(f"Tables to update: {table_names}")
print(f"Purge date: {purge_date}")

# Parse parameters
table_list = [t.strip() for t in table_names.split(',') if t.strip()]
print(f"Parsed {len(table_list)} tables")

# Parse validation result
try:
    validation_result = json.loads(validation_result_str)
    print(f"Validation result: {validation_result.get('validation_status', 'Unknown')}")
except:
    print("Warning: Could not parse validation result")
    validation_result = {}

# Format purge date for SQL (remove Z and microseconds)
purge_date_sql = purge_date.replace('Z', '').split('.')[0] if purge_date else datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
print(f"Purge date for SQL: {purge_date_sql}")

# ==========================================
# STEP 2: Create Pre-Purge Checkpoint
# ==========================================
print("\n" + "="*60)
print("STEP 2: Creating Pre-Purge Checkpoint")
print("="*60)

checkpoint_created = False
checkpoint_name = None
checkpoint_id = None

try:
    # Generate checkpoint name with purge date
    purge_date_formatted = purge_date_sql.split('T')[0].split(' ')[0].replace('-', '')  # YYYYMMDD
    checkpoint_name = f"pre_purge_{purge_date_formatted}"
    checkpoint_id = f"{pipeline_run_id}_pre_purge_checkpoint"
    
    print(f"Checkpoint ID: {checkpoint_id}")
    print(f"Checkpoint Name: {checkpoint_name}")
    
    # Calculate retention date (365 days from now)
    retention_date = (datetime.utcnow() + timedelta(days=365)).strftime('%Y-%m-%d')
    print(f"Retention date: {retention_date}")
    
    # Get Delta Lake version for each table for checkpoint metadata
    table_versions = []
    for table_name in table_list:
        try:
            # Get schema from metadata
            schema_df = spark.sql(f"""
                SELECT SchemaName 
                FROM metadata.PipelineConfig 
                WHERE TableName = '{table_name}'
            """)
            
            if schema_df.count() == 0:
                print(f"  Warning: No schema found for '{table_name}'")
                table_versions.append(f"{table_name}:no_schema")
                continue
            
            schema_name = schema_df.collect()[0]['SchemaName']
            full_table_name = f"{schema_name}.{table_name}"
            
            version_df = spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1")
            if version_df.count() > 0:
                version = version_df.collect()[0]['version']
                table_versions.append(f"{table_name}:v{version}")
                print(f"  Table '{full_table_name}' current version: {version}")
        except Exception as e:
            print(f"  Warning: Could not get version for '{table_name}': {str(e)}")
            table_versions.append(f"{table_name}:unknown")
    
    versions_str = "; ".join(table_versions)
    
    # Insert checkpoint record
    checkpoint_sql = f"""
        INSERT INTO metadata.CheckpointHistory (
            CheckpointId, 
            CheckpointName, 
            CheckpointType, 
            CreatedDate,
            TablesIncluded,
            TotalRows,
            ValidationStatus, 
            RetentionDate, 
            IsActive,
            PipelineRunId,
            SchemaName,
            TableName
        ) VALUES (
            '{checkpoint_id}',
            '{checkpoint_name}',
            'PrePurge',
            current_timestamp(),
            {len(table_list)},
            NULL,
            'Validated',
            DATE'{retention_date}',
            true,
            '{pipeline_run_id}',
            NULL,
            NULL
        )
    """

    spark.sql(checkpoint_sql)
    print(f"✓ Checkpoint '{checkpoint_name}' created successfully for {len(table_list)} tables")
    print(f"  Table versions: {versions_str}")
    
    checkpoint_created = True
    
except Exception as e:
    error_msg = f"Failed to create pre-purge checkpoint: {str(e)}"
    print(f"✗ {error_msg}")
    
    # Log checkpoint failure
    try:
        # Escape single quotes in error message for SQL
        error_msg_escaped = error_msg.replace("'", "''")
        spark.sql(f"""
            INSERT INTO metadata.SyncAuditLog (
                LogId, PipelineRunId, PipelineName, TableName, Operation,
                StartTime, EndTime, RowsProcessed, RowsDeleted, RowsPurged,
                Status, ErrorMessage, Notes, RetryCount, CreatedDate
            ) VALUES (
                '{pipeline_run_id}_checkpoint_failed',
                '{pipeline_run_id}',
                'PurgeMetadataUpdate',
                'Multiple',
                'CheckpointCreation',
                current_timestamp(),
                current_timestamp(),
                0, 0, 0,
                'Error',
                '{error_msg_escaped}',
                NULL,
                0,
                current_timestamp()
            )
        """)
    except Exception as log_error:
        print(f"Warning: Could not log error: {log_error}")
    
    # Raise exception to fail the activity (don't call exit on failure)
    raise Exception(f"Checkpoint creation failed: {error_msg}")

# ==========================================
# STEP 3: Update Metadata (Atomic Transaction)
# ==========================================
print("\n" + "="*60)
print("STEP 3: Updating Purge Metadata")
print("="*60)

updated_tables = []
update_errors = []

try:
    # Start transaction-like update (Delta Lake provides ACID guarantees)
    for table_name in table_list:
        try:
            print(f"\nUpdating metadata for: {table_name}")
            
            # Get current value
            current_df = spark.sql(f"""
                SELECT LastPurgeDate 
                FROM metadata.PipelineConfig 
                WHERE TableName = '{table_name}'
            """)
            
            if current_df.count() > 0:
                current_value = current_df.collect()[0]['LastPurgeDate']
                print(f"  Current LastPurgeDate: {current_value}")
            else:
                print(f"  Warning: No metadata record found (should not happen after validation)")
            
            # Update LastPurgeDate
            update_sql = f"""
                UPDATE metadata.PipelineConfig
                SET 
                    LastPurgeDate = TIMESTAMP'{purge_date_sql}',
                    ModifiedDate = current_timestamp()
                WHERE TableName = '{table_name}'
            """
            
            spark.sql(update_sql)
            
            # Verify update
            verify_df = spark.sql(f"""
                SELECT LastPurgeDate, ModifiedDate 
                FROM metadata.PipelineConfig 
                WHERE TableName = '{table_name}'
            """)
            
            if verify_df.count() > 0:
                new_record = verify_df.collect()[0]
                print(f"  ✓ Updated LastPurgeDate: {new_record['LastPurgeDate']}")
                print(f"  ✓ Updated ModifiedDate: {new_record['ModifiedDate']}")
                updated_tables.append(table_name)
            else:
                raise Exception(f"Verification failed: no record found after update")
            
        except Exception as e:
            error_msg = f"Failed to update metadata for '{table_name}': {str(e)}"
            print(f"  ✗ {error_msg}")
            update_errors.append(error_msg)
    
    # Check if all updates succeeded
    if len(update_errors) > 0:
        raise Exception(f"{len(update_errors)} table(s) failed to update")
    
    print(f"\n✓ Successfully updated metadata for {len(updated_tables)} table(s)")
    
except Exception as e:
    error_msg = f"Metadata update failed: {str(e)}"
    print(f"\n✗ {error_msg}")
    
    # Log metadata update failure
    try:
        error_summary = "; ".join(update_errors[:3]) if update_errors else "See error message"
        error_msg_escaped = error_msg.replace("'", "''")
        error_summary_escaped = error_summary.replace("'", "''")
        
        spark.sql(f"""
            INSERT INTO metadata.SyncAuditLog (
                LogId, PipelineRunId, PipelineName, TableName, Operation,
                StartTime, EndTime, RowsProcessed, RowsDeleted, RowsPurged,
                Status, ErrorMessage, Notes, RetryCount, CreatedDate
            ) VALUES (
                '{pipeline_run_id}_metadata_update_failed',
                '{pipeline_run_id}',
                'PurgeMetadataUpdate',
                'Multiple',
                'MetadataUpdate',
                current_timestamp(),
                current_timestamp(),
                0, 0, 0,
                'Error',
                '{error_msg_escaped}',
                'Errors: {error_summary_escaped}',
                0,
                current_timestamp()
            )
        """)
    except Exception as log_error:
        print(f"Warning: Could not log error: {log_error}")
    
    # Raise exception to fail the activity (don't call exit on failure)
    checkpoint_info = f"Checkpoint: {checkpoint_name}" if checkpoint_created else "No checkpoint created"
    updated_info = f"Updated tables: {updated_tables}" if updated_tables else "No tables updated"
    raise Exception(f"Metadata update failed: {error_msg}. {updated_info}. {checkpoint_info}")

# ==========================================
# STEP 4: Log Audit Trail (Per Table)
# ==========================================
print("\n" + "="*60)
print("STEP 4: Logging Audit Trail")
print("="*60)

audit_log_success = True

for table_name in updated_tables:
    try:
        log_id = f"{pipeline_run_id}_{table_name}_purge_recorded"
        
        audit_sql = f"""
            INSERT INTO metadata.SyncAuditLog (
                LogId, PipelineRunId, PipelineName, TableName, Operation,
                StartTime, EndTime, RowsProcessed, RowsDeleted, RowsPurged,
                Status, ErrorMessage, Notes, RetryCount, CreatedDate
            ) VALUES (
                '{log_id}',
                '{pipeline_run_id}',
                'PurgeMetadataUpdate',
                '{table_name}',
                'PurgeRecorded',
                current_timestamp(),
                current_timestamp(),
                0, 0, 0,
                'Success',
                NULL,
                'Purge date recorded: {purge_date_sql}. Next sync will classify missing records accordingly.',
                0,
                current_timestamp()
            )
        """
        
        spark.sql(audit_sql)
        print(f"✓ Audit log entry created for '{table_name}'")
        
    except Exception as e:
        print(f"✗ Warning: Failed to create audit log for '{table_name}': {str(e)}")
        audit_log_success = False

if audit_log_success:
    print(f"\n✓ Audit trail logged for all {len(updated_tables)} table(s)")
else:
    print(f"\n⚠ Audit trail partially logged (some entries failed)")

# ==========================================
# STEP 5: Return Success Result
# ==========================================
print("\n" + "="*60)
print("UPDATE COMPLETED SUCCESSFULLY")
print("="*60)

exit_value = {
    "update_status": "Success",
    "checkpoint_created": checkpoint_name,
    "checkpoint_id": checkpoint_id,
    "tables_updated": updated_tables,
    "table_count": len(updated_tables),
    "purge_date": purge_date_sql,
    "audit_logs_created": audit_log_success,
    "next_steps": "Next daily sync will classify missing records based on CreatedOn vs LastPurgeDate"
}

print(json.dumps(exit_value, indent=2))
mssparkutils.notebook.exit(json.dumps(exit_value))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
