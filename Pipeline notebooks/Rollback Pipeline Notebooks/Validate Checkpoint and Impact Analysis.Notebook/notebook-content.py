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
# Notebook 1: Validate Checkpoint & Generate Impact Analysis
# ============================================================
#
# PURPOSE: Validates checkpoint exists and generates comprehensive 
#          impact report showing what will change during rollback
#
# INPUTS (Pipeline Parameters - defined in PARAMETERS cell below):
#   - checkpoint_id: Unique CheckpointId to restore to (required)
#   - tables_scope: "All" or comma-separated table names (default: "All")
#   - pipeline_run_id: Unique identifier for this rollback operation
#
# OUTPUTS (exitValue JSON):
#   - validation_passed: true/false
#   - checkpoint_info: metadata about checkpoint (includes checkpoint_timestamp)
#   - impact_summary: aggregate metrics
#   - tables_affected: list of tables with individual impacts
#   - warnings: list of warning messages
#
# ============================================================

# CELL 1: PARAMETERS
# ============================================================
# Toggle this cell to "Parameters" type in MS Fabric
# Pipeline will inject parameter values into these variables
# ============================================================
# checkpoint_id = ""
# tables_scope = "All"
# pipeline_run_id = ""

# ============================================================
# CELL 2: IMPORTS AND SETUP
# ============================================================
import json
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from notebookutils import mssparkutils

# ==========================================
# STEP 0: Validate Input Parameters
# ==========================================
print("="*60)
print("ROLLBACK VALIDATION & IMPACT ANALYSIS")
print("="*60)

# Validate required parameters
if not checkpoint_id:
    error_msg = "checkpoint_id parameter is required"
    print("\n✗ {0}".format(error_msg))
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg,
        "checkpoint_info": {},
        "impact_summary": {},
        "tables_affected": [],
        "warnings": []
    }))

if not pipeline_run_id:
    pipeline_run_id = "rollback_{0}".format(datetime.now().strftime('%Y%m%d_%H%M%S'))

print("\nParameters:")
print("  Checkpoint ID: {0}".format(checkpoint_id))
print("  Tables Scope: {0}".format(tables_scope))
print("  Pipeline Run ID: {0}".format(pipeline_run_id))

# Parse tables_scope string into table list
if tables_scope.upper() == "ALL":
    table_list = None  # Will get all tables from PipelineConfig
    print("  → Rolling back ALL tables")
else:
    # Parse comma-separated table names
    table_list = [t.strip() for t in tables_scope.split(",") if t.strip()]
    print("  → Rolling back {0} specific table(s): {1}".format(len(table_list), ', '.join(table_list)))

warnings = []

# ==========================================
# STEP 1: Validate Checkpoint Exists and Is Valid
# ==========================================
print("\n" + "="*60)
print("STEP 1: Validating Checkpoint")
print("="*60)

try:
    # Query by CheckpointId (unique identifier)
    checkpoint_query = """
        SELECT 
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
        FROM metadata.CheckpointHistory
        WHERE CheckpointId = '{checkpoint_id}'
    """.format(checkpoint_id=checkpoint_id)
    
    checkpoint_df = spark.sql(checkpoint_query)
    
    if checkpoint_df.count() == 0:
        error_msg = "Checkpoint with ID '{0}' not found".format(checkpoint_id)
        print("\n✗ {0}".format(error_msg))
        
        # Log validation failure
        log_id = "{0}_validation_failed".format(pipeline_run_id)
        error_msg_escaped = error_msg.replace("'", "''")
        
        insert_log_query = """
            INSERT INTO metadata.SyncAuditLog (
                LogId, PipelineRunId, PipelineName, TableName, Operation,
                StartTime, EndTime, RowsProcessed, Status, ErrorMessage, RetryCount, CreatedDate
            ) VALUES (
                '{log_id}',
                '{pipeline_run_id}',
                'ManualRollback',
                NULL,
                'RollbackValidation',
                current_timestamp(),
                current_timestamp(),
                0,
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
    
    checkpoint = checkpoint_df.collect()[0]
    
    print("\n✓ Checkpoint found:")
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
    
    # Check if checkpoint passed validation
    if checkpoint.ValidationStatus != 'Validated':
        warning = "Checkpoint validation status is '{0}' (not 'Validated'). Proceed with caution.".format(
            checkpoint.ValidationStatus
        )
        warnings.append(warning)
        print("\n⚠ {0}".format(warning))
    
    print("\n✓ Checkpoint is valid and active")
    
except Exception as e:
    error_msg = "Failed to validate checkpoint: {0}".format(str(e))
    print("\n✗ {0}".format(error_msg))
    
    error_msg_escaped = str(e).replace("'", "''")
    log_id = "{0}_validation_failed".format(pipeline_run_id)
    
    insert_log_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, Operation, Status, ErrorMessage, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            'RollbackValidation',
            'Error',
            '{error_msg_escaped}',
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

# ==========================================
# STEP 2: Get List of Tables to Analyze
# ==========================================
print("\n" + "="*60)
print("STEP 2: Determining Tables in Scope")
print("="*60)

try:
    if table_list is None:
        # Get all enabled tables from PipelineConfig
        tables_query = """
            SELECT TableName, SchemaName, PrimaryKeyColumn
            FROM metadata.PipelineConfig
            WHERE SyncEnabled = true
            ORDER BY TableName
        """
        tables_df = spark.sql(tables_query)
        table_list = [row.TableName for row in tables_df.collect()]
        print("\n✓ Found {0} enabled tables in configuration".format(len(table_list)))
    else:
        # Validate specified tables exist in config
        valid_tables = []
        for table_name in table_list:
            check_query = """
                SELECT TableName 
                FROM metadata.PipelineConfig
                WHERE TableName = '{table_name}' AND SyncEnabled = true
            """.format(table_name=table_name)
            check_df = spark.sql(check_query)
            
            if check_df.count() > 0:
                valid_tables.append(table_name)
            else:
                warning = "Table '{0}' not found in configuration or not enabled - skipping".format(table_name)
                print("  ⚠ {0}".format(warning))
                warnings.append(warning)
        
        table_list = valid_tables
        if len(table_list) == 0:
            error_msg = "No valid tables found in scope"
            print("\n✗ {0}".format(error_msg))
            mssparkutils.notebook.exit(json.dumps({
                "validation_passed": False,
                "error": error_msg,
                "checkpoint_info": {},
                "impact_summary": {},
                "tables_affected": [],
                "warnings": warnings
            }))
        
        print("\n✓ Validated {0} table(s) in scope".format(len(table_list)))
    
    print("\nTables to analyze:")
    for table_name in table_list:
        print("  • {0}".format(table_name))

except Exception as e:
    error_msg = "Failed to determine tables in scope: {0}".format(str(e))
    print("\n✗ {0}".format(error_msg))
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg,
        "checkpoint_info": {},
        "impact_summary": {},
        "tables_affected": [],
        "warnings": warnings
    }))

# ==========================================
# STEP 3: Generate Impact Analysis for Each Table
# ==========================================
print("\n" + "="*60)
print("STEP 3: Generating Impact Analysis")
print("="*60)
print("Checkpoint Timestamp: {0}".format(checkpoint.CreatedDate))
print("="*60)

impact_analysis = []
total_current_rows = 0
total_checkpoint_rows = 0

for idx, table_name in enumerate(table_list, 1):
    print("\n[{0}/{1}] Analyzing: {2}".format(idx, len(table_list), table_name))
    
    try:
        # Get schema name from PipelineConfig
        schema_query = """
            SELECT SchemaName 
            FROM metadata.PipelineConfig 
            WHERE TableName = '{table_name}'
        """.format(table_name=table_name)
        schema_df = spark.sql(schema_query)
        
        if schema_df.count() == 0:
            warning = "No schema found for table '{0}' - skipping".format(table_name)
            warnings.append(warning)
            print("  ⚠ {0}".format(warning))
            continue
        
        schema_name = schema_df.collect()[0].SchemaName
        full_table_name = "{0}.{1}".format(schema_name, table_name)
        
        # Get current state
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
        
        # Get checkpoint state (at that timestamp)
        checkpoint_timestamp_str = checkpoint.CreatedDate.strftime('%Y-%m-%dT%H:%M:%S')
        
        checkpoint_query = """
            SELECT COUNT(*) as total_rows
            FROM {full_table_name}
            TIMESTAMP AS OF '{checkpoint_timestamp_str}'
        """.format(full_table_name=full_table_name, checkpoint_timestamp_str=checkpoint_timestamp_str)
        
        checkpoint_df = spark.sql(checkpoint_query)
        checkpoint_total = checkpoint_df.collect()[0].total_rows
        
        # Calculate impact
        rows_delta = checkpoint_total - current_total
        
        # Determine risk level
        if abs(rows_delta) > 10000:
            risk_level = "High"
        elif abs(rows_delta) > 1000:
            risk_level = "Medium"
        else:
            risk_level = "Low"
        
        # Calculate time span
        now = datetime.now()
        checkpoint_dt = checkpoint.CreatedDate
        time_span_hours = (now - checkpoint_dt).total_seconds() / 3600
        time_span_days = time_span_hours / 24
        
        if time_span_days >= 1:
            time_span = "{0:.1f} days".format(time_span_days)
        else:
            time_span = "{0:.1f} hours".format(time_span_hours)
        
        impact_analysis.append({
            "table_name": full_table_name,
            "current_total_rows": current_total,
            "current_active_rows": current_active,
            "current_deleted_rows": current_deleted,
            "current_purged_rows": current_purged,
            "checkpoint_total_rows": checkpoint_total,
            "rows_delta": rows_delta,
            "time_span": time_span,
            "risk_level": risk_level
        })
        
        total_current_rows += current_total
        total_checkpoint_rows += checkpoint_total
        
        print("  Current: {0:,} rows ({1:,} active, {2:,} deleted, {3:,} purged)".format(
            current_total, current_active, current_deleted, current_purged
        ))
        print("  Checkpoint: {0:,} rows".format(checkpoint_total))
        print("  Delta: {0:+,} rows".format(rows_delta))
        print("  Time span: {0}".format(time_span))
        print("  Risk: {0}".format(risk_level))
    
    except Exception as e:
        error_msg = "Failed to analyze table '{0}': {1}".format(table_name, str(e))
        print("  ✗ {0}".format(error_msg))
        warnings.append(error_msg)

# ==========================================
# STEP 4: Generate Summary and Log Results
# ==========================================
print("\n" + "="*60)
print("IMPACT ANALYSIS SUMMARY")
print("="*60)

total_delta = total_checkpoint_rows - total_current_rows
high_risk_count = len([t for t in impact_analysis if t["risk_level"] == "High"])
medium_risk_count = len([t for t in impact_analysis if t["risk_level"] == "Medium"])
low_risk_count = len([t for t in impact_analysis if t["risk_level"] == "Low"])

print("\n📊 Aggregate Metrics:")
print("  Tables Affected: {0}".format(len(impact_analysis)))
print("  Current Total Rows: {0:,}".format(total_current_rows))
print("  Checkpoint Total Rows: {0:,}".format(total_checkpoint_rows))
print("  Net Change: {0:+,} rows".format(total_delta))
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
    notes = "Impact analysis completed. {0} tables analyzed.".format(len(impact_analysis))
    notes_escaped = notes.replace("'", "''")
    
    insert_log_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, RowsProcessed, Status, Notes, RetryCount, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            'Multiple',
            'RollbackValidation',
            current_timestamp(),
            current_timestamp(),
            {total_checkpoint_rows},
            'Success',
            '{notes_escaped}',
            0,
            current_timestamp()
        )
    """.format(
        log_id=log_id,
        pipeline_run_id=pipeline_run_id,
        total_checkpoint_rows=total_checkpoint_rows,
        notes_escaped=notes_escaped
    )
    spark.sql(insert_log_query)
    
    print("\n✓ Validation logged to SyncAuditLog")
except Exception as e:
    print("\n⚠ Failed to log validation: {0}".format(str(e)))

# ==========================================
# STEP 5: Return Results
# ==========================================
result = {
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
        "high_risk_tables": high_risk_count,
        "medium_risk_tables": medium_risk_count,
        "low_risk_tables": low_risk_count
    },
    "tables_affected": [t["table_name"] for t in impact_analysis],
    "warnings": warnings
}

print("\n" + "="*60)
print("✓ VALIDATION COMPLETE - READY FOR ROLLBACK")
print("="*60)

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
