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
tables_scope = "All"
pipeline_run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CR-002: Manual Rollback to Checkpoint
# Notebook 1: Validate Checkpoint & Impact Analysis (Enhanced)
# ============================================================
#
# PURPOSE: Validates checkpoint exists, is active, and generates
#          detailed impact analysis showing expected state changes
#
# INPUTS (Pipeline Parameters):
#   - checkpoint_id: Unique CheckpointId to validate
#   - tables_scope: "All" or comma-separated table names
#   - pipeline_run_id: Unique identifier for this operation
#
# OUTPUTS (exitValue JSON):
#   - validation_passed: true/false
#   - checkpoint_info: metadata about checkpoint
#   - impact_summary: aggregate metrics
#   - tables_affected: list of table names
#   - warnings: any warnings encountered
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
print("ROLLBACK VALIDATION & IMPACT ANALYSIS")
print("="*60)

if not checkpoint_id:
    error_msg = "Missing required parameter: checkpoint_id"
    print("\n✗ {0}".format(error_msg))
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg
    }))

print("\nParameters:")
print("  Checkpoint ID: {0}".format(checkpoint_id))
print("  Tables Scope: {0}".format(tables_scope))
print("  Pipeline Run ID: {0}".format(pipeline_run_id))

if tables_scope != "All":
    print("  → Rolling back SPECIFIC tables: {0}".format(tables_scope))
else:
    print("  → Rolling back ALL tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 1: Validate Checkpoint Exists and Is Active
# ==========================================
print("\n" + "="*60)
print("STEP 1: Validating Checkpoint")
print("="*60)

checkpoint_query = """
    SELECT 
        CheckpointId, CheckpointName, CheckpointType, CreatedDate,
        TablesIncluded, TotalRows, ValidationStatus, IsActive, RetentionDate
    FROM metadata.CheckpointHistory
    WHERE CheckpointId = '{checkpoint_id}'
""".format(checkpoint_id=checkpoint_id)

checkpoint_result = spark.sql(checkpoint_query).collect()

if not checkpoint_result:
    error_msg = "Checkpoint ID '{0}' not found in CheckpointHistory".format(checkpoint_id)
    print("\n✗ {0}".format(error_msg))
    
    error_msg_escaped = error_msg.replace("'", "''")
    log_id = "{0}_validation_failed".format(pipeline_run_id)
    
    insert_log_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, Status, ErrorMessage, RetryCount, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            NULL,
            'RollbackValidation',
            current_timestamp(),
            current_timestamp(),
            'Error',
            '{error_msg_escaped}',
            0,
            current_timestamp()
        )
    """.format(
        log_id=log_id,
        pipeline_run_id=pipeline_run_id,
        error_msg_escaped=error_msg_escaped
    )
    spark.sql(insert_log_query)
    
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg,
        "checkpoint_info": {},
        "impact_summary": {},
        "tables_affected": [],
        "warnings": []
    }))

checkpoint = checkpoint_result[0]

print("✓ Checkpoint found:")
print("  ID: {0}".format(checkpoint.CheckpointId))
print("  Name: {0}".format(checkpoint.CheckpointName))
print("  Type: {0}".format(checkpoint.CheckpointType))
print("  Created: {0}".format(checkpoint.CreatedDate))
print("  Tables Included: {0}".format(checkpoint.TablesIncluded))
print("  Total Rows: {0:,}".format(checkpoint.TotalRows))
print("  Validation Status: {0}".format(checkpoint.ValidationStatus))

# Check if checkpoint is active
if not checkpoint.IsActive:
    error_msg = "Checkpoint ID '{0}' (Name: '{1}') is not active (expired or deactivated)".format(
        checkpoint_id, checkpoint.CheckpointName
    )
    print("\n✗ {0}".format(error_msg))
    print("  Retention Date: {0}".format(checkpoint.RetentionDate))
    
    error_msg_escaped = error_msg.replace("'", "''")
    log_id = "{0}_validation_failed".format(pipeline_run_id)
    
    insert_log_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, Status, ErrorMessage, RetryCount, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            NULL,
            'RollbackValidation',
            current_timestamp(),
            current_timestamp(),
            'Error',
            '{error_msg_escaped}',
            0,
            current_timestamp()
        )
    """.format(
        log_id=log_id,
        pipeline_run_id=pipeline_run_id,
        error_msg_escaped=error_msg_escaped
    )
    spark.sql(insert_log_query)
    
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg,
        "checkpoint_info": {
            "name": checkpoint.CheckpointName,
            "type": checkpoint.CheckpointType,
            "created_date": str(checkpoint.CreatedDate),
            "is_active": False
        },
        "impact_summary": {},
        "tables_affected": [],
        "warnings": []
    }))

print("\n✓ Checkpoint is valid and active")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 2: Determine Tables in Scope
# ==========================================
print("\n" + "="*60)
print("STEP 2: Determining Tables in Scope")
print("="*60)

if tables_scope == "All":
    tables_query = """
        SELECT SchemaName, TableName
        FROM metadata.PipelineConfig
        WHERE SyncEnabled = true
        ORDER BY TableName
    """
    tables_df = spark.sql(tables_query)
    tables_list = tables_df.collect()
else:
    # Parse comma-separated table names
    table_names = [t.strip() for t in tables_scope.split(',')]
    tables_list = []
    for table_name in table_names:
        try:
            table_query = """
                SELECT SchemaName, TableName
                FROM metadata.PipelineConfig
                WHERE TableName = '{table_name}'
                AND SyncEnabled = true
            """.format(table_name=table_name)
            result = spark.sql(table_query).collect()
            if result:
                tables_list.extend(result)
        except:
            pass

if not tables_list:
    error_msg = "No enabled tables found for scope: {0}".format(tables_scope)
    print("\n✗ {0}".format(error_msg))
    
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg,
        "checkpoint_info": {},
        "impact_summary": {},
        "tables_affected": [],
        "warnings": []
    }))

print("✓ Found {0} enabled tables in configuration".format(len(tables_list)))
print("\nTables to analyze:")
for table_row in tables_list:
    print("  • {0}.{1}".format(table_row.SchemaName, table_row.TableName))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 3: Generate Impact Analysis with Detailed State Breakdown
# ==========================================
print("\n" + "="*60)
print("STEP 3: Generating Impact Analysis")
print("="*60)
print("Checkpoint Timestamp: {0}".format(checkpoint.CreatedDate))
print("="*60)

impact_analysis = []
warnings = []
total_current_rows = 0
total_checkpoint_rows = 0
total_current_active = 0
total_checkpoint_active = 0
total_current_deleted = 0
total_checkpoint_deleted = 0
total_current_purged = 0
total_checkpoint_purged = 0

from delta.tables import DeltaTable
from pyspark.sql.functions import col as F_col, lit

for idx, table_row in enumerate(tables_list, 1):
    schema_name = table_row.SchemaName
    table_name = table_row.TableName
    
    print("\n[{0}/{1}] Analyzing: {2}".format(idx, len(tables_list), table_name))
    
    try:
        full_table_name = "{0}.{1}".format(schema_name, table_name)
        
        # Get DETAILED current state
        current_query = """
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = false AND COALESCE(IsPurged, false) = false THEN 1 ELSE 0 END) as active_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = true THEN 1 ELSE 0 END) as deleted_rows,
                SUM(CASE WHEN COALESCE(IsPurged, false) = true THEN 1 ELSE 0 END) as purged_rows
            FROM {full_table_name}
        """.format(full_table_name=full_table_name)
        
        current_df = spark.sql(current_query)
        current = current_df.collect()[0]
        
        current_total = current.total_rows if current.total_rows else 0
        current_active = current.active_rows if current.active_rows else 0
        current_deleted = current.deleted_rows if current.deleted_rows else 0
        current_purged = current.purged_rows if current.purged_rows else 0
        
        # Get Delta table and find appropriate version
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        # Find the latest version at or before checkpoint timestamp
        checkpoint_dt = checkpoint.CreatedDate
        
        version_history = delta_table.history() \
            .select("version", "timestamp") \
            .filter(F_col("timestamp") <= lit(checkpoint_dt)) \
            .orderBy(F_col("timestamp").desc()) \
            .first()
        
        if not version_history:
            warning_msg = "No Delta version found for {0} before checkpoint {1}".format(
                table_name, checkpoint_dt
            )
            print("  ⚠ {0}".format(warning_msg))
            warnings.append(warning_msg)
            continue
        
        checkpoint_version = version_history.version
        checkpoint_version_time = version_history.timestamp
        
        print("  Using version {0} (timestamp: {1})".format(
            checkpoint_version, checkpoint_version_time
        ))
        
        # Query using VERSION AS OF (more reliable than timestamp)
        checkpoint_query = """
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = false AND COALESCE(IsPurged, false) = false THEN 1 ELSE 0 END) as active_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = true THEN 1 ELSE 0 END) as deleted_rows,
                SUM(CASE WHEN COALESCE(IsPurged, false) = true THEN 1 ELSE 0 END) as purged_rows
            FROM {full_table_name}
            VERSION AS OF {version}
        """.format(full_table_name=full_table_name, version=checkpoint_version)
        
        checkpoint_df = spark.sql(checkpoint_query)
        checkpoint_row = checkpoint_df.collect()[0]
        
        checkpoint_total = checkpoint_row.total_rows if checkpoint_row.total_rows else 0
        checkpoint_active = checkpoint_row.active_rows if checkpoint_row.active_rows else 0
        checkpoint_deleted = checkpoint_row.deleted_rows if checkpoint_row.deleted_rows else 0
        checkpoint_purged = checkpoint_row.purged_rows if checkpoint_row.purged_rows else 0
        
        # Calculate ALL deltas
        rows_delta = checkpoint_total - current_total
        active_delta = checkpoint_active - current_active
        deleted_delta = checkpoint_deleted - current_deleted
        purged_delta = checkpoint_purged - current_purged
        
        # Determine risk level based on total change magnitude
        if abs(rows_delta) > 10000:
            risk_level = "High"
        elif abs(rows_delta) > 1000:
            risk_level = "Medium"
        else:
            risk_level = "Low"
        
        # Calculate time span
        now = datetime.now()
        time_span_hours = (now - checkpoint_dt).total_seconds() / 3600
        time_span_days = time_span_hours / 24
        
        if time_span_days >= 1:
            time_span = "{0:.1f} days".format(time_span_days)
        else:
            time_span = "{0:.1f} hours".format(time_span_hours)
        
        impact_analysis.append({
            "table_name": full_table_name,
            "delta_version_used": checkpoint_version,
            "delta_version_timestamp": str(checkpoint_version_time),
            "current_total_rows": current_total,
            "current_active_rows": current_active,
            "current_deleted_rows": current_deleted,
            "current_purged_rows": current_purged,
            "checkpoint_total_rows": checkpoint_total,
            "checkpoint_active_rows": checkpoint_active,
            "checkpoint_deleted_rows": checkpoint_deleted,
            "checkpoint_purged_rows": checkpoint_purged,
            "rows_delta": rows_delta,
            "active_delta": active_delta,
            "deleted_delta": deleted_delta,
            "purged_delta": purged_delta,
            "risk_level": risk_level,
            "time_span": time_span
        })
        
        # Accumulate totals
        total_current_rows += current_total
        total_checkpoint_rows += checkpoint_total
        total_current_active += current_active
        total_checkpoint_active += checkpoint_active
        total_current_deleted += current_deleted
        total_checkpoint_deleted += checkpoint_deleted
        total_current_purged += current_purged
        total_checkpoint_purged += checkpoint_purged
        
        print("  ✓ Analysis complete")
        print("    Current: {0:,} total ({1:,} active, {2:,} deleted, {3:,} purged)".format(
            current_total, current_active, current_deleted, current_purged
        ))
        print("    Checkpoint: {0:,} total ({1:,} active, {2:,} deleted, {3:,} purged)".format(
            checkpoint_total, checkpoint_active, checkpoint_deleted, checkpoint_purged
        ))
        print("    Delta: {0:+,} total ({1:+,} active, {2:+,} deleted, {3:+,} purged)".format(
            rows_delta, active_delta, deleted_delta, purged_delta
        ))
        print("    Risk: {0}".format(risk_level))
        
    except Exception as e:
        error_msg = "Failed to analyze table '{0}': {1}".format(table_name, str(e))
        print("  ✗ {0}".format(error_msg))
        warnings.append(error_msg)
        continue

print("\n" + "="*60)
print("Impact analysis completed for {0}/{1} tables".format(
    len(impact_analysis), len(tables_list)
))
if len(warnings) > 0:
    print("⚠ {0} warning(s) encountered".format(len(warnings)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 4: Generate Enhanced Summary and Log Results
# ==========================================
print("\n" + "="*60)
print("IMPACT ANALYSIS SUMMARY")
print("="*60)

total_delta = total_checkpoint_rows - total_current_rows
active_delta_total = total_checkpoint_active - total_current_active
deleted_delta_total = total_checkpoint_deleted - total_current_deleted
purged_delta_total = total_checkpoint_purged - total_current_purged

high_risk_count = len([t for t in impact_analysis if t["risk_level"] == "High"])
medium_risk_count = len([t for t in impact_analysis if t["risk_level"] == "Medium"])
low_risk_count = len([t for t in impact_analysis if t["risk_level"] == "Low"])

print("\n📊 Aggregate Metrics:")
print("  Tables Affected: {0}".format(len(impact_analysis)))
print("  Current Total Rows: {0:,}".format(total_current_rows))
print("  Checkpoint Total Rows: {0:,}".format(total_checkpoint_rows))
print("  Net Change: {0:+,} rows".format(total_delta))

print("\n📊 Expected State Changes:")
print("  Active: {0:+,} ({1:,} → {2:,})".format(
    active_delta_total, total_current_active, total_checkpoint_active
))
print("  Deleted: {0:+,} ({1:,} → {2:,})".format(
    deleted_delta_total, total_current_deleted, total_checkpoint_deleted
))
print("  Purged: {0:+,} ({1:,} → {2:,})".format(
    purged_delta_total, total_current_purged, total_checkpoint_purged
))

print("\n⚠ Risk Distribution:")
print("  High Risk: {0} table(s)".format(high_risk_count))
print("  Medium Risk: {0} table(s)".format(medium_risk_count))
print("  Low Risk: {0} table(s)".format(low_risk_count))

if len(warnings) > 0:
    print("\n⚠ Warnings ({0}):".format(len(warnings)))
    for warning in warnings:
        print("  • {0}".format(warning))

# Log validation success
try:
    log_id = "{0}_validation_success".format(pipeline_run_id)
    notes = "Impact analysis completed. {0} tables analyzed. Expected changes: {1:+,} active, {2:+,} deleted, {3:+,} purged".format(
        len(impact_analysis), active_delta_total, deleted_delta_total, purged_delta_total
    )
    notes_escaped = notes.replace("'", "''")
    
    insert_log_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, Status, Notes, RetryCount, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            NULL,
            'RollbackImpactAnalysis',
            current_timestamp(),
            current_timestamp(),
            'Success',
            '{notes_escaped}',
            0,
            current_timestamp()
        )
    """.format(
        log_id=log_id,
        pipeline_run_id=pipeline_run_id,
        notes_escaped=notes_escaped
    )
    spark.sql(insert_log_query)
    
    print("\n✓ Validation logged to SyncAuditLog")

except Exception as e:
    print("\n⚠ Failed to log validation: {0}".format(str(e)))

# ==========================================
# Final Output
# ==========================================
print("\n" + "="*60)
print("✓ VALIDATION COMPLETE - READY FOR ROLLBACK")
print("="*60)

# Prepare tables_affected list (full schema.table names)
tables_affected = [t["table_name"] for t in impact_analysis]

# Return comprehensive results
exit_value = {
    "validation_passed": True,
    "checkpoint_info": {
        "id": checkpoint.CheckpointId,
        "name": checkpoint.CheckpointName,
        "type": checkpoint.CheckpointType,
        "created_date": checkpoint.CreatedDate.strftime('%Y-%m-%dT%H:%M:%S'),
        "tables_included": checkpoint.TablesIncluded,
        "total_rows": checkpoint.TotalRows,
        "validation_status": checkpoint.ValidationStatus
    },
    "impact_summary": {
        "tables_affected": len(impact_analysis),
        "current_total_rows": total_current_rows,
        "checkpoint_total_rows": total_checkpoint_rows,
        "net_change_rows": total_delta,
        "active_change": active_delta_total,
        "deleted_change": deleted_delta_total,
        "purged_change": purged_delta_total,
        "high_risk_tables": high_risk_count,
        "medium_risk_tables": medium_risk_count,
        "low_risk_tables": low_risk_count
    },
    "tables_affected": tables_affected,
    "warnings": warnings
}

mssparkutils.notebook.exit(json.dumps(exit_value))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
