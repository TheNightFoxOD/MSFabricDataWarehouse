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
# META       "default_lakehouse_workspace_id": "b0f83c07-a701-49bb-a165-e06ca0ee4000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# ============================================================
# CELL 1: PARAMETERS
# ============================================================
# Toggle this cell to "Parameters" type in MS Fabric
# Pipeline will inject parameter values into these variables
# ============================================================
checkpoint_id = ""
checkpoint_name = ""
checkpoint_timestamp = ""
tables_json = "[]"
pipeline_run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CR-002: Manual Rollback to Checkpoint
# Notebook 2: Execute Rollback with RESTORE TABLE
# ============================================================
#
# PURPOSE: Captures pre-rollback state, executes Delta Lake RESTORE
#          for each table, captures detailed post-restore state
#
# INPUTS (Pipeline Parameters):
#   - checkpoint_id: Unique CheckpointId (for audit trail)
#   - checkpoint_name: Checkpoint name (for display/logging)
#   - checkpoint_timestamp: Timestamp to restore to
#   - tables_json: JSON array of table names to restore
#   - pipeline_run_id: Unique identifier for this operation
#
# OUTPUTS (exitValue JSON):
#   - execution_passed: true/false
#   - tables_succeeded: count of successful restorations
#   - tables_failed: count of failed restorations
#   - restoration_details: detailed per-table results with counts
#   - pre_rollback_snapshots: pre-state for all tables
#
# ============================================================

# ============================================================
# CELL 2: IMPORTS AND SETUP
# ============================================================
import json
from datetime import datetime
from pyspark.sql.functions import col
from notebookutils import mssparkutils

# ==========================================
# STEP 0: Validate Input Parameters
# ==========================================
print("="*60)
print("ROLLBACK EXECUTION - RESTORE TABLE")
print("="*60)

# Validate required parameters
if not checkpoint_id or not checkpoint_timestamp or not tables_json:
    error_msg = "Missing required parameters: checkpoint_id, checkpoint_timestamp, or tables_json"
    print("\n✗ {0}".format(error_msg))
    mssparkutils.notebook.exit(json.dumps({
        "execution_passed": False,
        "error": error_msg,
        "tables_succeeded": 0,
        "tables_failed": 0,
        "restoration_details": []
    }))

try:
    table_list = json.loads(tables_json)
except Exception as e:
    error_msg = "Failed to parse tables_json: {0}".format(str(e))
    print("\n✗ {0}".format(error_msg))
    mssparkutils.notebook.exit(json.dumps({
        "execution_passed": False,
        "error": error_msg,
        "tables_succeeded": 0,
        "tables_failed": 0,
        "restoration_details": []
    }))

print("\nParameters:")
print("  Checkpoint ID: {0}".format(checkpoint_id))
print("  Checkpoint Name: {0}".format(checkpoint_name))
print("  Checkpoint Timestamp: {0}".format(checkpoint_timestamp))
print("  Tables to Restore: {0}".format(len(table_list)))
print("  Pipeline Run ID: {0}".format(pipeline_run_id))

# ==========================================
# STEP 1: Capture Pre-Rollback State for All Tables
# ==========================================
print("\n" + "="*60)
print("STEP 1: Capturing Pre-Rollback State")
print("="*60)

pre_rollback_snapshots = []
snapshot_failures = []

for table_name in table_list:
    try:
        print("\nCapturing state: {0}".format(table_name))
        
        # Get comprehensive state metrics
        # CRITICAL: Use COALESCE to handle NULL values (NULL = false for boolean flags)
        state_query = """
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = false AND COALESCE(IsPurged, false) = false THEN 1 ELSE 0 END) as active_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = true THEN 1 ELSE 0 END) as deleted_rows,
                SUM(CASE WHEN COALESCE(IsPurged, false) = true THEN 1 ELSE 0 END) as purged_rows
            FROM {table_name}
        """.format(table_name=table_name)
        
        state_df = spark.sql(state_query)
        state_row = state_df.collect()[0]
        
        total_rows = state_row.total_rows
        active_rows = state_row.active_rows if state_row.active_rows else 0
        deleted_rows = state_row.deleted_rows if state_row.deleted_rows else 0
        purged_rows = state_row.purged_rows if state_row.purged_rows else 0
        
        # Get Delta version
        try:
            version_df = spark.sql("DESCRIBE DETAIL {0}".format(table_name))
            delta_version = version_df.select("version").collect()[0].version
        except:
            delta_version = None
        
        # Get sample record IDs (first 10 for validation)
        try:
            config_query = """
                SELECT PrimaryKeyColumn 
                FROM metadata.PipelineConfig 
                WHERE TableName = '{table_name}'
            """.format(table_name=table_name.split('.')[-1])
            config_result = spark.sql(config_query).collect()
            
            if config_result:
                pk_col = config_result[0].PrimaryKeyColumn
                sample_query = """
                    SELECT {pk_col} 
                    FROM {table_name} 
                    LIMIT 10
                """.format(pk_col=pk_col, table_name=table_name)
                sample_ids_df = spark.sql(sample_query)
                sample_ids = json.dumps([row[0] for row in sample_ids_df.collect()])
            else:
                sample_ids = None
        except:
            sample_ids = None
        
        # Insert snapshot into RollbackStateSnapshots
        snapshot_id = "{0}_{1}_pre".format(pipeline_run_id, table_name.replace('.', '_'))
        sample_ids_value = "'{0}'".format(sample_ids.replace("'", "''")) if sample_ids else "NULL"
        delta_version_value = delta_version if delta_version is not None else "NULL"
        
        insert_snapshot_query = """
            INSERT INTO metadata.RollbackStateSnapshots (
                SnapshotId, PipelineRunId, TableName, SnapshotType, SnapshotDate,
                TotalRows, ActiveRows, DeletedRows, PurgedRows,
                DeltaVersion, SampleRecordIds, CreatedDate
            ) VALUES (
                '{snapshot_id}',
                '{pipeline_run_id}',
                '{table_name}',
                'PreRollback',
                current_timestamp(),
                {total_rows},
                {active_rows},
                {deleted_rows},
                {purged_rows},
                {delta_version},
                {sample_ids},
                current_timestamp()
            )
        """.format(
            snapshot_id=snapshot_id,
            pipeline_run_id=pipeline_run_id,
            table_name=table_name,
            total_rows=total_rows,
            active_rows=active_rows,
            deleted_rows=deleted_rows,
            purged_rows=purged_rows,
            delta_version=delta_version_value,
            sample_ids=sample_ids_value
        )
        spark.sql(insert_snapshot_query)
        
        pre_rollback_snapshots.append({
            "table_name": table_name,
            "total_rows": total_rows,
            "active_rows": active_rows,
            "deleted_rows": deleted_rows,
            "purged_rows": purged_rows,
            "delta_version": delta_version
        })
        
        print("  ✓ State captured: {0:,} total ({1:,} active, {2:,} deleted, {3:,} purged)".format(
            total_rows, active_rows, deleted_rows, purged_rows
        ))
    
    except Exception as e:
        error = "Failed to capture pre-rollback state for '{0}': {1}".format(table_name, str(e))
        print("  ✗ {0}".format(error))
        snapshot_failures.append(error)

if snapshot_failures:
    print("\n⚠ Warning: Failed to capture state for {0} table(s)".format(len(snapshot_failures)))
    print("Continuing with rollback execution...")

print("\n✓ Pre-rollback state captured for {0}/{1} table(s)".format(
    len(pre_rollback_snapshots), len(table_list)
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 2: Execute Delta Lake RESTORE for Each Table
# ==========================================
print("\n" + "="*60)
print("STEP 2: Executing Delta Lake RESTORE")
print("="*60)
print("Target Timestamp: {0}".format(checkpoint_timestamp))
print("="*60)

restoration_details = []
tables_succeeded = 0
tables_failed = 0

for idx, table_name in enumerate(table_list, 1):
    print("\n[{0}/{1}] Restoring: {2}".format(idx, len(table_list), table_name))
    
    restore_start = datetime.now()
    
    try:
        # Execute RESTORE TABLE command
        restore_command = "RESTORE TABLE {0} TO TIMESTAMP AS OF '{1}'".format(
            table_name, checkpoint_timestamp
        )
        
        print("  Command: {0}".format(restore_command))
        
        spark.sql(restore_command)
        
        restore_end = datetime.now()
        duration_seconds = (restore_end - restore_start).total_seconds()
        
        # Get DETAILED post-restore state (total + breakdown)
        # CRITICAL: Use COALESCE to handle NULL values (NULL = false for boolean flags)
        post_state_query = """
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = false AND COALESCE(IsPurged, false) = false THEN 1 ELSE 0 END) as active_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = true THEN 1 ELSE 0 END) as deleted_rows,
                SUM(CASE WHEN COALESCE(IsPurged, false) = true THEN 1 ELSE 0 END) as purged_rows
            FROM {table_name}
        """.format(table_name=table_name)
        
        post_state_df = spark.sql(post_state_query)
        post_state_row = post_state_df.collect()[0]
        
        post_total = post_state_row.total_rows
        post_active = post_state_row.active_rows if post_state_row.active_rows else 0
        post_deleted = post_state_row.deleted_rows if post_state_row.deleted_rows else 0
        post_purged = post_state_row.purged_rows if post_state_row.purged_rows else 0
        
        # Get new Delta version
        try:
            version_df = spark.sql("DESCRIBE DETAIL {0}".format(table_name))
            new_version = version_df.select("version").collect()[0].version
        except:
            new_version = None
        
        print("  ✓ Restore successful!")
        print("    Duration: {0:.2f} seconds".format(duration_seconds))
        print("    Post-restore: {0:,} total ({1:,} active, {2:,} deleted, {3:,} purged)".format(
            post_total, post_active, post_deleted, post_purged
        ))
        print("    New Delta version: {0}".format(new_version))
        
        # Log success to audit trail
        log_id = "{0}_{1}_restore".format(pipeline_run_id, table_name.replace('.', '_'))
        notes = "Restored to checkpoint: {0}. Duration: {1:.2f}s".format(
            checkpoint_name, duration_seconds
        )
        notes_escaped = notes.replace("'", "''")
        
        insert_success_query = """
            INSERT INTO metadata.SyncAuditLog (
                LogId, PipelineRunId, PipelineName, TableName, Operation,
                StartTime, EndTime, RowsProcessed, Status, Notes, RetryCount, CreatedDate
            ) VALUES (
                '{log_id}',
                '{pipeline_run_id}',
                'ManualRollback',
                '{table_name}',
                'TableRestore',
                timestamp'{start_time}',
                timestamp'{end_time}',
                {post_total},
                'Success',
                '{notes_escaped}',
                0,
                current_timestamp()
            )
        """.format(
            log_id=log_id,
            pipeline_run_id=pipeline_run_id,
            table_name=table_name,
            start_time=restore_start.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=restore_end.strftime('%Y-%m-%d %H:%M:%S'),
            post_total=post_total,
            notes_escaped=notes_escaped
        )
        spark.sql(insert_success_query)
        
        # Store detailed restoration result
        restoration_details.append({
            "table_name": table_name,
            "status": "Success",
            "duration_seconds": round(duration_seconds, 2),
            "post_restore_total": post_total,
            "post_restore_active": post_active,
            "post_restore_deleted": post_deleted,
            "post_restore_purged": post_purged,
            "new_delta_version": new_version,
            "error": None
        })
        
        tables_succeeded += 1
    
    except Exception as e:
        restore_end = datetime.now()
        duration_seconds = (restore_end - restore_start).total_seconds()
        error_msg = str(e)
        
        print("  ✗ Restore failed: {0}".format(error_msg))
        
        # Log failure to audit trail
        error_msg_escaped = error_msg.replace("'", "''")
        log_id = "{0}_{1}_restore".format(pipeline_run_id, table_name.replace('.', '_'))
        
        insert_error_query = """
            INSERT INTO metadata.SyncAuditLog (
                LogId, PipelineRunId, PipelineName, TableName, Operation,
                StartTime, EndTime, RowsProcessed, Status, ErrorMessage, RetryCount, CreatedDate
            ) VALUES (
                '{log_id}',
                '{pipeline_run_id}',
                'ManualRollback',
                '{table_name}',
                'TableRestore',
                timestamp'{start_time}',
                timestamp'{end_time}',
                0,
                'Error',
                '{error_msg_escaped}',
                0,
                current_timestamp()
            )
        """.format(
            log_id=log_id,
            pipeline_run_id=pipeline_run_id,
            table_name=table_name,
            start_time=restore_start.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=restore_end.strftime('%Y-%m-%d %H:%M:%S'),
            error_msg_escaped=error_msg_escaped
        )
        spark.sql(insert_error_query)
        
        restoration_details.append({
            "table_name": table_name,
            "status": "Failed",
            "duration_seconds": round(duration_seconds, 2),
            "post_restore_total": None,
            "post_restore_active": None,
            "post_restore_deleted": None,
            "post_restore_purged": None,
            "new_delta_version": None,
            "error": error_msg
        })
        
        tables_failed += 1
        print("  → Continuing with next table...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 3: Generate Execution Summary
# ==========================================
print("\n" + "="*60)
print("ROLLBACK EXECUTION COMPLETE")
print("="*60)

total_duration = sum(d["duration_seconds"] for d in restoration_details)

print("\n📊 EXECUTION SUMMARY:")
print("  Tables Attempted: {0}".format(len(table_list)))
print("  Tables Succeeded: {0}".format(tables_succeeded))
print("  Tables Failed: {0}".format(tables_failed))
print("  Success Rate: {0:.1f}%".format(tables_succeeded/len(table_list)*100 if len(table_list) > 0 else 0))
print("  Total Duration: {0:.2f} seconds ({1:.1f} minutes)".format(total_duration, total_duration/60))

if tables_failed > 0:
    print("\n⚠ Failed Tables:")
    for detail in restoration_details:
        if detail["status"] == "Failed":
            print("  • {0}: {1}".format(detail["table_name"], detail["error"]))

execution_passed = tables_failed == 0

if execution_passed:
    print("\n✓ All tables restored successfully")
else:
    print("\n⚠ Partial success: {0}/{1} tables restored".format(tables_succeeded, len(table_list)))

# ==========================================
# STEP 4: Return Results
# ==========================================
result = {
    "execution_passed": execution_passed,
    "tables_attempted": len(table_list),
    "tables_succeeded": tables_succeeded,
    "tables_failed": tables_failed,
    "success_rate": round(tables_succeeded/len(table_list)*100, 1) if len(table_list) > 0 else 0,
    "total_duration_seconds": round(total_duration, 2),
    "restoration_details": restoration_details,
    "pre_rollback_snapshots": pre_rollback_snapshots
}

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }