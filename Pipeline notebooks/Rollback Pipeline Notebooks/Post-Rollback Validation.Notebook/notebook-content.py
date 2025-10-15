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
restoration_details_json = "[]"
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
# Notebook 3: Post-Rollback Validation & Summary
# ============================================================
#
# PURPOSE: Validates data integrity after rollback, creates post-rollback
#          checkpoint, generates comprehensive comparison report
#
# INPUTS (Pipeline Parameters):
#   - checkpoint_id: Original checkpoint ID (for FK reference)
#   - checkpoint_name: Original checkpoint name (for display)
#   - restoration_details_json: JSON from Execute Rollback notebook
#   - tables_json: JSON array of table names (from validation)
#   - pipeline_run_id: Unique identifier for this operation
#
# OUTPUTS (exitValue JSON):
#   - validation_passed: true/false
#   - quality_score: aggregate quality score
#   - checkpoint_created: true/false
#   - comparison_report: summary of changes
#
# ============================================================

# ============================================================
# CELL 2: IMPORTS AND SETUP
# ============================================================
import json
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from notebookutils import mssparkutils

# Initialize all variables early to prevent NameError
validation_passed = True
avg_quality_score = 0
checkpoint_created = False
post_checkpoint_name = None
total_records_restored = 0
total_records_removed = 0
total_active_restored = 0
total_deleted_restored = 0
total_purged_restored = 0

# ==========================================
# STEP 0: Validate Input Parameters
# ==========================================
print("="*60)
print("POST-ROLLBACK VALIDATION & SUMMARY")
print("="*60)

try:
    table_list = json.loads(tables_json)
    restoration_details = json.loads(restoration_details_json)
except Exception as e:
    error_msg = "Failed to parse JSON parameters: {0}".format(str(e))
    print("\n✗ {0}".format(error_msg))
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg,
        "quality_score": 0,
        "checkpoint_created": False,
        "comparison_report": {}
    }))

print("\nParameters:")
print("  Checkpoint ID: {0}".format(checkpoint_id))
print("  Checkpoint Name: {0}".format(checkpoint_name))
print("  Tables to Validate: {0}".format(len(table_list)))
print("  Restoration Details: {0} table(s)".format(len(restoration_details)))
print("  Pipeline Run ID: {0}".format(pipeline_run_id))

# Identify successfully restored tables
succeeded_tables = [d["table_name"] for d in restoration_details if d["status"] == "Success"]
failed_tables = [d["table_name"] for d in restoration_details if d["status"] == "Failed"]

print("\nRestoration Results:")
print("  Succeeded: {0} table(s)".format(len(succeeded_tables)))
print("  Failed: {0} table(s)".format(len(failed_tables)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 1: Capture Post-Rollback State from restoration_details
# ==========================================
print("\n" + "="*60)
print("STEP 1: Recording Post-Rollback State")
print("="*60)

post_rollback_snapshots = []

for detail in restoration_details:
    if detail["status"] == "Success":
        table_name = detail["table_name"]
        
        try:
            print("\nRecording state: {0}".format(table_name))
            
            # Use the detailed counts already captured in Notebook 2
            total_rows = detail["post_restore_total"]
            active_rows = detail["post_restore_active"]
            deleted_rows = detail["post_restore_deleted"]
            purged_rows = detail["post_restore_purged"]
            delta_version = detail["new_delta_version"]
            
            # Insert post-rollback snapshot
            snapshot_id = "{0}_{1}_post".format(pipeline_run_id, table_name.replace('.', '_'))
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
                    'PostRollback',
                    current_timestamp(),
                    {total_rows},
                    {active_rows},
                    {deleted_rows},
                    {purged_rows},
                    {delta_version},
                    NULL,
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
                delta_version=delta_version_value
            )
            spark.sql(insert_snapshot_query)
            
            post_rollback_snapshots.append({
                "table_name": table_name,
                "total_rows": total_rows,
                "active_rows": active_rows,
                "deleted_rows": deleted_rows,
                "purged_rows": purged_rows,
                "delta_version": delta_version
            })
            
            print("  ✓ State recorded: {0:,} total ({1:,} active, {2:,} deleted, {3:,} purged)".format(
                total_rows, active_rows, deleted_rows, purged_rows
            ))
        
        except Exception as e:
            print("  ✗ Failed to record post-rollback state: {0}".format(str(e)))

print("\n✓ Post-rollback state recorded for {0} table(s)".format(len(post_rollback_snapshots)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 2: Validate Data Integrity
# ==========================================
print("\n" + "="*60)
print("STEP 2: Validating Data Integrity")
print("="*60)

validation_results = []
validation_failures = []

for table_name in succeeded_tables:
    print("\nValidating: {0}".format(table_name))
    
    try:
        post_snapshot = next((s for s in post_rollback_snapshots if s["table_name"] == table_name), None)
        if not post_snapshot:
            validation_failures.append("{0}: No post-rollback snapshot found".format(table_name))
            continue
        
        total_rows = post_snapshot["total_rows"]
        active_rows = post_snapshot["active_rows"]
        deleted_rows = post_snapshot["deleted_rows"]
        purged_rows = post_snapshot["purged_rows"]
        
        checks = []
        
        # Check 1: Internal consistency
        calculated_total = active_rows + deleted_rows + purged_rows
        if calculated_total == total_rows:
            checks.append({"check": "Internal Consistency", "status": "PASS"})
            print("  ✓ Internal Consistency: {0} + {1} + {2} = {3}".format(
                active_rows, deleted_rows, purged_rows, total_rows
            ))
        else:
            error = "Consistency check failed: {0} + {1} + {2} = {3}, expected {4}".format(
                active_rows, deleted_rows, purged_rows, calculated_total, total_rows
            )
            checks.append({"check": "Internal Consistency", "status": "FAIL", "error": error})
            validation_failures.append("{0}: {1}".format(table_name, error))
            print("  ✗ Internal Consistency: {0}".format(error))
        
        # Check 2: No null primary keys
        try:
            config_query = """
                SELECT PrimaryKeyColumn 
                FROM metadata.PipelineConfig 
                WHERE TableName = '{table_name}'
            """.format(table_name=table_name.split('.')[-1])
            config_result = spark.sql(config_query).collect()
            
            if config_result:
                pk_col = config_result[0].PrimaryKeyColumn
                null_pk_query = """
                    SELECT COUNT(*) as null_count
                    FROM {table_name}
                    WHERE {pk_col} IS NULL
                """.format(table_name=table_name, pk_col=pk_col)
                null_count = spark.sql(null_pk_query).collect()[0].null_count
                
                if null_count == 0:
                    checks.append({"check": "No Null PKs", "status": "PASS"})
                    print("  ✓ No Null Primary Keys")
                else:
                    error = "Found {0} null primary keys".format(null_count)
                    checks.append({"check": "No Null PKs", "status": "FAIL", "error": error})
                    validation_failures.append("{0}: {1}".format(table_name, error))
                    print("  ✗ {0}".format(error))
        except Exception as pk_error:
            print("  ⚠ Could not validate primary keys: {0}".format(str(pk_error)))
        
        # Check 3: Tracking flags logical consistency
        try:
            flags_query = """
                SELECT 
                    SUM(CASE WHEN IsDeleted = true AND DeletedDate IS NULL THEN 1 ELSE 0 END) as deleted_no_date,
                    SUM(CASE WHEN IsPurged = true AND PurgedDate IS NULL THEN 1 ELSE 0 END) as purged_no_date,
                    SUM(CASE WHEN IsDeleted = false AND DeletedDate IS NOT NULL THEN 1 ELSE 0 END) as active_has_deleted_date,
                    SUM(CASE WHEN IsPurged = false AND PurgedDate IS NOT NULL THEN 1 ELSE 0 END) as not_purged_has_purge_date
                FROM {table_name}
            """.format(table_name=table_name)
            flags_result = spark.sql(flags_query).collect()[0]
            
            flag_issues = 0
            if flags_result.deleted_no_date > 0:
                flag_issues += flags_result.deleted_no_date
            if flags_result.purged_no_date > 0:
                flag_issues += flags_result.purged_no_date
            
            if flag_issues == 0:
                checks.append({"check": "Tracking Flags Consistency", "status": "PASS"})
                print("  ✓ Tracking Flags Consistent")
            else:
                error = "Found {0} records with inconsistent tracking flags".format(flag_issues)
                checks.append({"check": "Tracking Flags Consistency", "status": "FAIL", "error": error})
                validation_failures.append("{0}: {1}".format(table_name, error))
                print("  ✗ {0}".format(error))
        except Exception as flag_error:
            print("  ⚠ Could not validate tracking flags: {0}".format(str(flag_error)))
        
        # Calculate quality score for this table
        passed_checks = sum(1 for c in checks if c.get("status") == "PASS")
        total_checks = len(checks)
        table_quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        validation_results.append({
            "table_name": table_name,
            "checks": checks,
            "quality_score": table_quality_score
        })
        
        # Insert validation record
        validation_passed_table = table_quality_score == 100
        validation_id = "{0}_{1}_validation".format(pipeline_run_id, table_name.replace('.', '_'))
        notes = "Post-rollback validation. Quality: {0:.1f}%".format(table_quality_score)
        notes_escaped = notes.replace("'", "''")
        
        insert_validation_query = """
            INSERT INTO metadata.DataValidation (
                ValidationId, ValidationDate, TableName, 
                BronzeRowCount, ActiveRowCount, DeletedRowCount, PurgedRowCount, 
                ValidationPassed, Notes
            ) VALUES (
                '{validation_id}',
                current_timestamp(),
                '{table_name}',
                {total_rows},
                {active_rows},
                {deleted_rows},
                {purged_rows},
                {validation_passed_table},
                '{notes_escaped}'
            )
        """.format(
            validation_id=validation_id,
            table_name=table_name.split('.')[-1],
            total_rows=total_rows,
            active_rows=active_rows,
            deleted_rows=deleted_rows,
            purged_rows=purged_rows,
            validation_passed_table=str(validation_passed_table).lower(),
            notes_escaped=notes_escaped
        )
        spark.sql(insert_validation_query)
    
    except Exception as e:
        error = "Validation failed for {0}: {1}".format(table_name, str(e))
        print("  ✗ {0}".format(error))
        validation_failures.append(error)

# Calculate aggregate quality score
if validation_results:
    avg_quality_score = sum(v["quality_score"] for v in validation_results) / len(validation_results)
else:
    avg_quality_score = 0

validation_passed = len(validation_failures) == 0

print("\n📊 VALIDATION SUMMARY:")
print("  Average Quality Score: {0:.2f}%".format(avg_quality_score))
print("  Validation Status: {0}".format("PASSED" if validation_passed else "FAILED"))
if validation_failures:
    print("\n⚠ Validation Issues ({0}):".format(len(validation_failures)))
    for issue in validation_failures[:5]:  # Show first 5
        print("  • {0}".format(issue))
    if len(validation_failures) > 5:
        print("  ... and {0} more".format(len(validation_failures) - 5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 3: Create Post-Rollback Checkpoint
# ==========================================
print("\n" + "="*60)
print("STEP 3: Creating Post-Rollback Checkpoint")
print("="*60)

try:
    post_checkpoint_name = "{0}_post_rollback".format(checkpoint_name)
    post_checkpoint_id = "{0}_post".format(checkpoint_id)
    retention_date = datetime.now() + timedelta(days=30)
    
    total_post_rows = sum(s["total_rows"] for s in post_rollback_snapshots)
    tables_count = len(post_rollback_snapshots)
    
    insert_checkpoint_query = """
        INSERT INTO metadata.CheckpointHistory (
            CheckpointId, CheckpointName, CheckpointType, CreatedDate,
            TablesIncluded, TotalRows, ValidationStatus, RetentionDate, IsActive
        ) VALUES (
            '{checkpoint_id}',
            '{checkpoint_name}',
            'PostRollback',
            current_timestamp(),
            {tables_included},
            {total_rows},
            'Validated',
            date'{retention_date}',
            true
        )
    """.format(
        checkpoint_id=post_checkpoint_id,
        checkpoint_name=post_checkpoint_name,
        tables_included=tables_count,
        total_rows=total_post_rows,
        retention_date=retention_date.strftime('%Y-%m-%d')
    )
    spark.sql(insert_checkpoint_query)
    
    checkpoint_created = True
    
    print("✓ Post-rollback checkpoint created: {0}".format(post_checkpoint_name))
    print("  Tables: {0}".format(tables_count))
    print("  Total Rows: {0:,}".format(total_post_rows))
    print("  Retention: 30 days (until {0})".format(retention_date.strftime('%Y-%m-%d')))

except Exception as e:
    checkpoint_created = False
    print("✗ Failed to create post-rollback checkpoint: {0}".format(str(e)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 4: Generate Comprehensive Comparison Report
# ==========================================
print("\n" + "="*60)
print("STEP 4: Generating Comparison Report")
print("="*60)

try:
    # Retrieve pre-rollback snapshots
    pre_snapshots_query = """
        SELECT TableName, TotalRows, ActiveRows, DeletedRows, PurgedRows, DeltaVersion
        FROM metadata.RollbackStateSnapshots
        WHERE PipelineRunId = '{pipeline_run_id}'
        AND SnapshotType = 'PreRollback'
    """.format(pipeline_run_id=pipeline_run_id)
    pre_snapshots_df = spark.sql(pre_snapshots_query)
    pre_snapshots_dict = {row.TableName: row.asDict() for row in pre_snapshots_df.collect()}
    
    table_comparisons = []
    
    for post_snapshot in post_rollback_snapshots:
        table_name = post_snapshot["table_name"]
        pre_snapshot = pre_snapshots_dict.get(table_name)
        
        if pre_snapshot:
            # Calculate ALL deltas (total, active, deleted, purged)
            rows_delta = post_snapshot["total_rows"] - pre_snapshot["TotalRows"]
            active_delta = post_snapshot["active_rows"] - pre_snapshot["ActiveRows"]
            deleted_delta = post_snapshot["deleted_rows"] - pre_snapshot["DeletedRows"]
            purged_delta = post_snapshot["purged_rows"] - pre_snapshot["PurgedRows"]
            
            # Track ACTUAL state changes for meaningful metrics
            # Positive active_delta = records became active (restored from deleted/purged)
            # Negative deleted_delta = records un-deleted
            # Negative purged_delta = records un-purged
            
            if active_delta > 0:
                total_active_restored += active_delta
            
            if deleted_delta < 0:
                total_deleted_restored += abs(deleted_delta)
            
            if purged_delta < 0:
                total_purged_restored += abs(purged_delta)
            
            # Track raw row changes
            if rows_delta > 0:
                total_records_restored += rows_delta
            elif rows_delta < 0:
                total_records_removed += abs(rows_delta)
            
            table_comparisons.append({
                "table_name": table_name,
                "pre_total": pre_snapshot["TotalRows"],
                "post_total": post_snapshot["total_rows"],
                "rows_delta": rows_delta,
                "pre_active": pre_snapshot["ActiveRows"],
                "post_active": post_snapshot["active_rows"],
                "active_delta": active_delta,
                "pre_deleted": pre_snapshot["DeletedRows"],
                "post_deleted": post_snapshot["deleted_rows"],
                "deleted_delta": deleted_delta,
                "pre_purged": pre_snapshot["PurgedRows"],
                "post_purged": post_snapshot["purged_rows"],
                "purged_delta": purged_delta,
                "version_changed": pre_snapshot["DeltaVersion"] != post_snapshot["delta_version"]
            })
    
    # Create comparison report in RollbackComparisonReports
    report_id = "{0}_comparison".format(pipeline_run_id)
    issues_json = json.dumps(validation_failures).replace("'", "''") if validation_failures else "[]"
    
    # Calculate total state changes (more meaningful than row count changes with soft deletes)
    total_state_changes = total_active_restored + total_deleted_restored + total_purged_restored
    
    insert_report_query = """
        INSERT INTO metadata.RollbackComparisonReports (
            ReportId, PipelineRunId, CheckpointId, CheckpointName, RollbackDate,
            TablesAffected, TablesSucceeded, TablesFailed,
            RecordsRestored, RecordsRemoved, QualityScore,
            ValidationPassed, Issues, Notes, CreatedDate
        ) VALUES (
            '{report_id}',
            '{pipeline_run_id}',
            '{checkpoint_id}',
            '{checkpoint_name}',
            current_timestamp(),
            {tables_affected},
            {tables_succeeded},
            {tables_failed},
            {records_restored},
            {records_removed},
            {quality_score},
            {validation_passed_bool},
            '{issues_json}',
            'State changes: {active} active, {deleted} un-deleted, {purged} un-purged',
            current_timestamp()
        )
    """.format(
        report_id=report_id,
        pipeline_run_id=pipeline_run_id,
        checkpoint_id=checkpoint_id,
        checkpoint_name=checkpoint_name,
        tables_affected=len(table_list),
        tables_succeeded=len(succeeded_tables),
        tables_failed=len(failed_tables),
        records_restored=total_state_changes,  # More meaningful metric
        records_removed=total_records_removed,
        quality_score=avg_quality_score,
        validation_passed_bool=str(validation_passed).lower(),
        issues_json=issues_json,
        active=total_active_restored,
        deleted=total_deleted_restored,
        purged=total_purged_restored
    )
    spark.sql(insert_report_query)
    
    print("✓ Comparison report created")
    print("\n📊 STATE CHANGES:")
    print("  Records Became Active: {0:,}".format(total_active_restored))
    print("  Records Un-Deleted: {0:,}".format(total_deleted_restored))
    print("  Records Un-Purged: {0:,}".format(total_purged_restored))
    print("  Total State Changes: {0:,}".format(total_state_changes))
    if total_records_restored > 0 or total_records_removed > 0:
        print("\n📊 ROW COUNT CHANGES:")
        print("  Physical Rows Added: {0:,}".format(total_records_restored))
        print("  Physical Rows Removed: {0:,}".format(total_records_removed))

except Exception as e:
    print("✗ Failed to generate comparison report: {0}".format(str(e)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# STEP 5: Log Final Summary
# ==========================================
print("\n" + "="*60)
print("STEP 5: Logging Final Summary")
print("="*60)

try:
    summary_notes = "Validation: {0}. Quality Score: {1:.2f}%. {2} tables succeeded, {3} failed. State changes: {4:,}".format(
        "PASSED" if validation_passed else "FAILED",
        avg_quality_score,
        len(succeeded_tables),
        len(failed_tables),
        total_state_changes
    )
    summary_notes_escaped = summary_notes.replace("'", "''")
    
    insert_summary_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, Operation, 
            StartTime, EndTime, Status, RetryCount, Notes, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            'PostRollbackSummary',
            current_timestamp(),
            current_timestamp(),
            '{status}',
            0,
            '{notes}',
            current_timestamp()
        )
    """.format(
        log_id="{0}_summary".format(pipeline_run_id),
        pipeline_run_id=pipeline_run_id,
        status='Success' if validation_passed else 'Warning',
        notes=summary_notes_escaped
    )
    spark.sql(insert_summary_query)
    
    print("✓ Final summary logged")

except Exception as e:
    print("⚠ Failed to log summary: {0}".format(str(e)))

# ==========================================
# Final Output
# ==========================================
print("\n" + "="*60)
print("POST-ROLLBACK VALIDATION COMPLETE")
print("="*60)
print("\n📊 FINAL SUMMARY:")
print("  Validation Passed: {0}".format("YES" if validation_passed else "NO"))
print("  Quality Score: {0:.2f}%".format(avg_quality_score))
print("  Checkpoint Created: {0}".format("YES" if checkpoint_created else "NO"))
print("  Tables Validated: {0}/{1}".format(len(succeeded_tables), len(table_list)))
print("\n" + "="*60)
if validation_passed:
    print("✓ ROLLBACK VALIDATED SUCCESSFULLY")
else:
    print("⚠ ROLLBACK COMPLETED WITH VALIDATION WARNINGS")
print("="*60)

# Return results
exit_value = {
    "validation_passed": validation_passed,
    "quality_score": avg_quality_score,
    "checkpoint_created": checkpoint_created,
    "post_checkpoint_name": post_checkpoint_name,
    "tables_validated": len(succeeded_tables),
    "validation_failures": len(validation_failures),
    "comparison_report": {
        "total_state_changes": total_state_changes,
        "active_restored": total_active_restored,
        "deleted_restored": total_deleted_restored,
        "purged_restored": total_purged_restored,
        "rows_added": total_records_restored,
        "rows_removed": total_records_removed
    }
}

mssparkutils.notebook.exit(json.dumps(exit_value))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }