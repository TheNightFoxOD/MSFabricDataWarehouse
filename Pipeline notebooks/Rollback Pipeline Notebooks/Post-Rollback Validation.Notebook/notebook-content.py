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

# ============================================================
# CR-002: Manual Rollback to Checkpoint
# Notebook 3: Post-Rollback Validation & Summary
# ============================================================
#
# PURPOSE: Validates data integrity after rollback, creates post-rollback
#          checkpoint, generates comparison report
#
# INPUTS (Pipeline Parameters - defined in PARAMETERS cell below):
#   - checkpoint_id: Unique CheckpointId restored to (required)
#   - checkpoint_name: Checkpoint name for display/logging (required)
#   - restoration_details_json: JSON from execution notebook
#   - tables_json: JSON array of table names
#   - pipeline_run_id: Unique identifier for this rollback operation
#
# OUTPUTS (exitValue JSON):
#   - validation_passed: true/false
#   - quality_score: 0-100
#   - checkpoint_created: true/false
#   - comparison_report: detailed analysis
#
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
#          checkpoint, generates comparison report
#
# INPUTS (Pipeline Parameters - defined in PARAMETERS cell below):
#   - checkpoint_id: Original checkpoint ID (for foreign key reference)
#   - checkpoint_name: Original checkpoint name (for display)
#   - restoration_details_json: JSON from Execute Rollback notebook
#   - tables_json: JSON array of table names (from validation notebook)
#   - pipeline_run_id: Unique identifier for this rollback operation
#
# OUTPUTS (exitValue JSON):
#   - validation_passed: true/false
#   - quality_score: aggregate quality score
#   - checkpoint_created: true/false
#   - comparison_report: summary of changes
#
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

# ==========================================
# STEP 1: Capture Post-Rollback State
# ==========================================
print("\n" + "="*60)
print("STEP 1: Capturing Post-Rollback State")
print("="*60)

post_rollback_snapshots = []

for table_name in succeeded_tables:
    try:
        print("\nCapturing state: {0}".format(table_name))
        
        # Get current statistics with COALESCE for NULL handling
        query = """
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = false AND COALESCE(IsPurged, false) = false THEN 1 ELSE 0 END) as active_rows,
                SUM(CASE WHEN COALESCE(IsDeleted, false) = true THEN 1 ELSE 0 END) as deleted_rows,
                SUM(CASE WHEN COALESCE(IsPurged, false) = true THEN 1 ELSE 0 END) as purged_rows
            FROM {table_name}
        """.format(table_name=table_name)
        stats_df = spark.sql(query)
        
        stats = stats_df.collect()[0]
        total_rows = stats.total_rows if stats.total_rows else 0
        active_rows = stats.active_rows if stats.active_rows else 0
        deleted_rows = stats.deleted_rows if stats.deleted_rows else 0
        purged_rows = stats.purged_rows if stats.purged_rows else 0
        
        # Get Delta version
        try:
            version_df = spark.sql("DESCRIBE DETAIL {0}".format(table_name))
            delta_version = version_df.select("version").collect()[0].version
        except:
            delta_version = None
        
        # Insert into RollbackStateSnapshots
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
        
        print("  ✓ State captured: {0:,} total rows (Version {1})".format(total_rows, delta_version))
    
    except Exception as e:
        print("  ✗ Failed to capture post-rollback state: {0}".format(str(e)))

print("\n✓ Post-rollback state captured for {0} table(s)".format(len(post_rollback_snapshots)))

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
            config_df = spark.sql(config_query)
            
            if config_df.count() > 0:
                pk_column = config_df.collect()[0].PrimaryKeyColumn
                null_pk_query = "SELECT COUNT(*) as cnt FROM {0} WHERE {1} IS NULL".format(
                    table_name, pk_column
                )
                null_pk_df = spark.sql(null_pk_query)
                null_pk_count = null_pk_df.collect()[0].cnt
                
                if null_pk_count == 0:
                    checks.append({"check": "Primary Key Integrity", "status": "PASS"})
                    print("  ✓ Primary Key Integrity: No null PKs")
                else:
                    error = "{0} records with null primary key".format(null_pk_count)
                    checks.append({"check": "Primary Key Integrity", "status": "FAIL", "error": error})
                    validation_failures.append("{0}: {1}".format(table_name, error))
                    print("  ✗ Primary Key Integrity: {0}".format(error))
        except Exception as pk_error:
            checks.append({"check": "Primary Key Integrity", "status": "SKIP", "error": str(pk_error)})
            print("  ⚠ Primary Key check skipped: {0}".format(str(pk_error)))
        
        # Check 3: Tracking flag consistency
        try:
            flag_query = """
                SELECT COUNT(*) as cnt 
                FROM {table_name}
                WHERE COALESCE(IsDeleted, false) = true AND DeletedDate IS NULL
            """.format(table_name=table_name)
            flag_df = spark.sql(flag_query)
            invalid_flag_count = flag_df.collect()[0].cnt
            
            if invalid_flag_count == 0:
                checks.append({"check": "Tracking Flag Consistency", "status": "PASS"})
                print("  ✓ Tracking Flags: Consistent")
            else:
                error = "{0} records with IsDeleted=true but DeletedDate=NULL".format(invalid_flag_count)
                checks.append({"check": "Tracking Flag Consistency", "status": "FAIL", "error": error})
                validation_failures.append("{0}: {1}".format(table_name, error))
                print("  ✗ Tracking Flags: {0}".format(error))
        except:
            checks.append({"check": "Tracking Flag Consistency", "status": "SKIP"})
        
        # Calculate quality score for this table
        total_checks = len([c for c in checks if c["status"] in ["PASS", "FAIL"]])
        passed_checks = len([c for c in checks if c["status"] == "PASS"])
        table_quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        # Create DataValidation entry with Notes column
        validation_id = "{0}_{1}_postrollback".format(pipeline_run_id, table_name.replace('.', '_'))
        validation_passed_bool = len([c for c in checks if c["status"] == "FAIL"]) == 0
        notes_json = json.dumps(checks).replace("'", "''")
        
        insert_validation_query = """
            INSERT INTO metadata.DataValidation (
                ValidationId, ValidationDate, TableName,
                BronzeRowCount, ActiveRowCount, DeletedRowCount, PurgedRowCount,
                ValidationPassed, Notes, ValidationContext
            ) VALUES (
                '{validation_id}',
                current_timestamp(),
                '{table_name}',
                {total_rows},
                {active_rows},
                {deleted_rows},
                {purged_rows},
                {validation_passed},
                '{notes_json}',
                'PostRollback'
            )
        """.format(
            validation_id=validation_id,
            table_name=table_name,
            total_rows=total_rows,
            active_rows=active_rows,
            deleted_rows=deleted_rows,
            purged_rows=purged_rows,
            validation_passed=str(validation_passed_bool).lower(),
            notes_json=notes_json
        )
        spark.sql(insert_validation_query)
        
        validation_results.append({
            "table_name": table_name,
            "quality_score": round(table_quality_score, 2),
            "validation_passed": validation_passed_bool,
            "checks": checks
        })
        
        print("  Quality Score: {0:.2f}%".format(table_quality_score))
    
    except Exception as e:
        error = "Validation error for '{0}': {1}".format(table_name, str(e))
        print("  ✗ {0}".format(error))
        validation_failures.append(error)

# Calculate final validation status
if len(validation_failures) > 0:
    validation_passed = False

if validation_results:
    avg_quality_score = round(sum(v["quality_score"] for v in validation_results)/len(validation_results), 2)

# ==========================================
# STEP 3: Create Post-Rollback Checkpoint
# ==========================================
print("\n" + "="*60)
print("STEP 3: Creating Post-Rollback Checkpoint")
print("="*60)

try:
    post_checkpoint_name = "{0}_post_rollback".format(checkpoint_name)
    post_checkpoint_id = "{0}_post_checkpoint".format(pipeline_run_id)
    retention_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    total_rows_restored = sum(s["total_rows"] for s in post_rollback_snapshots) if post_rollback_snapshots else 0

    insert_checkpoint_query = """
        INSERT INTO metadata.CheckpointHistory (
            CheckpointId, CheckpointName, CheckpointType, CreatedDate,
            TablesIncluded, TotalRows, ValidationStatus, RetentionDate,
            IsActive, PipelineRunId, SchemaName, TableName
        ) VALUES (
            '{post_checkpoint_id}',
            '{post_checkpoint_name}',
            'PostRollback',
            current_timestamp(),
            {tables_included},
            {total_rows},
            '{validation_status}',
            date'{retention_date}',
            true,
            '{pipeline_run_id}',
            NULL,
            NULL
        )
    """.format(
        post_checkpoint_id=post_checkpoint_id,
        post_checkpoint_name=post_checkpoint_name,
        tables_included=len(succeeded_tables),
        total_rows=total_rows_restored,
        validation_status="Validated" if validation_passed else "ValidationFailed",
        retention_date=retention_date,
        pipeline_run_id=pipeline_run_id
    )
    spark.sql(insert_checkpoint_query)
    
    checkpoint_created = True
    print("✓ Post-rollback checkpoint created: {0}".format(post_checkpoint_name))
    print("  Tables: {0}".format(len(succeeded_tables)))
    print("  Total Rows: {0:,}".format(total_rows_restored))
    print("  Retention: 30 days (until {0})".format(retention_date))

except Exception as e:
    print("✗ Failed to create post-rollback checkpoint: {0}".format(str(e)))

# ==========================================
# STEP 4: Generate Comparison Report
# ==========================================
print("\n" + "="*60)
print("STEP 4: Generating Comparison Report")
print("="*60)

try:
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
            rows_delta = post_snapshot["total_rows"] - pre_snapshot["TotalRows"]
            active_delta = post_snapshot["active_rows"] - pre_snapshot["ActiveRows"]
            
            if rows_delta > 0:
                total_records_restored += rows_delta
            else:
                total_records_removed += abs(rows_delta)
            
            table_comparisons.append({
                "table_name": table_name,
                "pre_total": pre_snapshot["TotalRows"],
                "post_total": post_snapshot["total_rows"],
                "rows_delta": rows_delta,
                "pre_active": pre_snapshot["ActiveRows"],
                "post_active": post_snapshot["active_rows"],
                "active_delta": active_delta,
                "version_changed": pre_snapshot["DeltaVersion"] != post_snapshot["delta_version"]
            })
    
    # Create comparison report in RollbackComparisonReports
    report_id = "{0}_comparison".format(pipeline_run_id)
    issues_json = json.dumps(validation_failures).replace("'", "''") if validation_failures else "[]"

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
            'Rollback execution completed with full state comparison',
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
        records_restored=total_records_restored,
        records_removed=total_records_removed,
        quality_score=avg_quality_score,
        validation_passed_bool=str(validation_passed).lower(),
        issues_json=issues_json
    )
    spark.sql(insert_report_query)
    
    print("✓ Comparison report created")
    print("\n📊 DATA CHANGES:")
    print("  Records Restored: {0:,}".format(total_records_restored))
    print("  Records Removed: {0:,}".format(total_records_removed))
    print("  Net Change: {0:+,}".format(total_records_restored - total_records_removed))

except Exception as e:
    print("✗ Failed to generate comparison report: {0}".format(str(e)))

# ==========================================
# STEP 5: Log Final Summary
# ==========================================
print("\n" + "="*60)
print("STEP 5: Logging Final Summary")
print("="*60)

try:
    summary_notes = "Validation: {0}. Quality Score: {1:.2f}%. {2} tables succeeded, {3} failed.".format(
        'PASS' if validation_passed else 'FAIL',
        avg_quality_score,
        len(succeeded_tables),
        len(failed_tables)
    )
    summary_notes_escaped = summary_notes.replace("'", "''")
    log_id = "{0}_final_summary".format(pipeline_run_id)
    
    insert_summary_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, RowsProcessed, Status, Notes, RetryCount, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            'Multiple',
            'PostRollbackValidation',
            current_timestamp(),
            current_timestamp(),
            {total_rows},
            '{status}',
            '{notes_escaped}',
            0,
            current_timestamp()
        )
    """.format(
        log_id=log_id,
        pipeline_run_id=pipeline_run_id,
        total_rows=total_rows_restored,
        status='Success' if validation_passed else 'Warning',
        notes_escaped=summary_notes_escaped
    )
    spark.sql(insert_summary_query)
    
    print("✓ Final summary logged")

except Exception as e:
    print("⚠ Failed to log final summary: {0}".format(str(e)))

# ==========================================
# STEP 6: Return Results
# ==========================================
print("\n" + "="*60)
print("POST-ROLLBACK VALIDATION COMPLETE")
print("="*60)

print("\n📊 FINAL SUMMARY:")
print("  Validation Passed: {0}".format('YES' if validation_passed else 'NO'))
print("  Quality Score: {0:.2f}%".format(avg_quality_score))
print("  Checkpoint Created: {0}".format('YES' if checkpoint_created else 'NO'))
print("  Tables Validated: {0}/{1}".format(len(succeeded_tables), len(table_list)))

if validation_failures:
    print("\n⚠ Validation Issues ({0}):".format(len(validation_failures)))
    for issue in validation_failures[:5]:
        print("  • {0}".format(issue))
    if len(validation_failures) > 5:
        print("  ... and {0} more".format(len(validation_failures) - 5))

result = {
    "validation_passed": validation_passed,
    "quality_score": avg_quality_score,
    "checkpoint_created": checkpoint_created,
    "post_checkpoint_name": post_checkpoint_name,
    "tables_validated": len(succeeded_tables),
    "validation_failures": len(validation_failures),
    "comparison_report": {
        "records_restored": total_records_restored,
        "records_removed": total_records_removed,
        "net_change": total_records_restored - total_records_removed
    }
}

print("\n" + "="*60)
if validation_passed:
    print("✓ ROLLBACK VALIDATED SUCCESSFULLY")
else:
    print("⚠ ROLLBACK COMPLETED WITH VALIDATION WARNINGS")
print("="*60)

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
