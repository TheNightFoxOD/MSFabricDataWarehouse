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
print("POST-ROLLBACK VALIDATION & SUMMARY")
print("="*60)

try:
    table_list = json.loads(tables_json)
    restoration_details = json.loads(restoration_details_json)
except Exception as e:
    error_msg = f"Failed to parse JSON parameters: {str(e)}"
    print(f"\n✗ {error_msg}")
    mssparkutils.notebook.exit(json.dumps({
        "validation_passed": False,
        "error": error_msg,
        "quality_score": 0,
        "checkpoint_created": False,
        "comparison_report": {}
    }))

print(f"\nParameters:")
print(f"  Checkpoint ID: {checkpoint_id}")
print(f"  Checkpoint Name: {checkpoint_name}")
print(f"  Tables to Validate: {len(table_list)}")
print(f"  Restoration Details: {len(restoration_details)} table(s)")
print(f"  Pipeline Run ID: {pipeline_run_id}")

# Identify successfully restored tables
succeeded_tables = [d["table_name"] for d in restoration_details if d["status"] == "Success"]
failed_tables = [d["table_name"] for d in restoration_details if d["status"] == "Failed"]

print(f"\nRestoration Results:")
print(f"  Succeeded: {len(succeeded_tables)} table(s)")
print(f"  Failed: {len(failed_tables)} table(s)")

# ==========================================
# STEP 1: Capture Post-Rollback State
# ==========================================
print("\n" + "="*60)
print("STEP 1: Capturing Post-Rollback State")
print("="*60)

post_rollback_snapshots = []

for table_name in succeeded_tables:
    try:
        print(f"\nCapturing state: {table_name}")
        
        # Get current statistics
        query = """
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN IsDeleted = false AND IsPurged = false THEN 1 ELSE 0 END) as active_rows,
                SUM(CASE WHEN IsDeleted = true THEN 1 ELSE 0 END) as deleted_rows,
                SUM(CASE WHEN IsPurged = true THEN 1 ELSE 0 END) as purged_rows
            FROM {table_name}
        """.format(table_name=table_name)
        stats_df = spark.sql(query)
        
        stats = stats_df.collect()[0]
        total_rows = stats.total_rows if stats.total_rows else 0
        active_rows = stats.active_rows if stats.active_rows else 0
        deleted_rows = stats.deleted_rows if stats.deleted_rows else 0
        purged_rows = stats.purged_rows if stats.purged_rows else 0
        
        # Get Delta Lake version
        try:
            version_df = spark.sql("DESCRIBE DETAIL {0}".format(table_name))
            delta_version = version_df.select("version").collect()[0].version
        except:
            delta_version = None
        
        # Get sample record IDs
        try:
            config_query = """
                SELECT PrimaryKeyColumn 
                FROM metadata.PipelineConfig 
                WHERE TableName = '{table_name}'
            """.format(table_name=table_name)
            config_df = spark.sql(config_query)
            pk_column = config_df.collect()[0].PrimaryKeyColumn if config_df.count() > 0 else None
            
            if pk_column:
                sample_query = """
                    SELECT {pk_column} 
                    FROM {table_name} 
                    LIMIT 100
                """.format(table_name=table_name, pk_column=pk_column)
                sample_df = spark.sql(sample_query)
                sample_ids = [str(row[0]) for row in sample_df.collect()]
            else:
                sample_ids = []
        except:
            sample_ids = []
        
        snapshot_id = f"{pipeline_run_id}_{table_name}_post"
        sample_ids_json = json.dumps(sample_ids).replace("'", "''")
        
        insert_query = """
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
                '{sample_ids_json}',
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
            delta_version=delta_version if delta_version is not None else 'NULL',
            sample_ids_json=sample_ids_json
        )
        spark.sql(insert_query)
        
        post_rollback_snapshots.append({
            "table_name": table_name,
            "total_rows": total_rows,
            "active_rows": active_rows,
            "deleted_rows": deleted_rows,
            "purged_rows": purged_rows,
            "delta_version": delta_version
        })
        
        print(f"  ✓ State captured: {total_rows:,} total rows (Version {delta_version})")
    
    except Exception as e:
        print(f"  ✗ Failed to capture post-rollback state: {str(e)}")

print(f"\n✓ Post-rollback state captured for {len(post_rollback_snapshots)} table(s)")

# ==========================================
# STEP 2: Validate Data Integrity
# ==========================================
print("\n" + "="*60)
print("STEP 2: Validating Data Integrity")
print("="*60)

validation_results = []
validation_failures = []

for table_name in succeeded_tables:
    print(f"\nValidating: {table_name}")
    
    try:
        post_snapshot = next((s for s in post_rollback_snapshots if s["table_name"] == table_name), None)
        if not post_snapshot:
            validation_failures.append(f"{table_name}: No post-rollback snapshot found")
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
            print(f"  ✓ Internal Consistency: {active_rows} + {deleted_rows} + {purged_rows} = {total_rows}")
        else:
            error = f"Consistency check failed: {active_rows} + {deleted_rows} + {purged_rows} = {calculated_total}, expected {total_rows}"
            checks.append({"check": "Internal Consistency", "status": "FAIL", "error": error})
            validation_failures.append(f"{table_name}: {error}")
            print(f"  ✗ Internal Consistency: {error}")
        
        # Check 2: No null primary keys
        try:
            config_query = """
                SELECT PrimaryKeyColumn 
                FROM metadata.PipelineConfig 
                WHERE TableName = '{table_name}'
            """.format(table_name=table_name)
            config_df = spark.sql(config_query)
            pk_column = config_df.collect()[0].PrimaryKeyColumn if config_df.count() > 0 else None
            
            if pk_column:
                null_pk_query = """
                    SELECT COUNT(*) as null_count 
                    FROM {table_name} 
                    WHERE {pk_column} IS NULL
                """.format(table_name=table_name, pk_column=pk_column)
                null_pk_df = spark.sql(null_pk_query)
                null_count = null_pk_df.collect()[0].null_count
                
                if null_count == 0:
                    checks.append({"check": "No Null Primary Keys", "status": "PASS"})
                    print("  ✓ No Null Primary Keys")
                else:
                    error = f"Found {null_count} null primary keys"
                    checks.append({"check": "No Null Primary Keys", "status": "FAIL", "error": error})
                    validation_failures.append(f"{table_name}: {error}")
                    print(f"  ✗ No Null Primary Keys: {error}")
        except Exception as e:
            checks.append({"check": "No Null Primary Keys", "status": "SKIP", "error": str(e)})
        
        # Check 3: Tracking columns consistency
        inconsistent_query = """
            SELECT COUNT(*) as inconsistent_count
            FROM {table_name}
            WHERE (IsDeleted = true AND IsPurged = true)
        """.format(table_name=table_name)
        inconsistent_df = spark.sql(inconsistent_query)
        inconsistent_count = inconsistent_df.collect()[0].inconsistent_count
        
        if inconsistent_count == 0:
            checks.append({"check": "Tracking Column Consistency", "status": "PASS"})
            print("  ✓ Tracking Column Consistency")
        else:
            error = f"Found {inconsistent_count} records with inconsistent tracking flags"
            checks.append({"check": "Tracking Column Consistency", "status": "FAIL", "error": error})
            validation_failures.append(f"{table_name}: {error}")
            print(f"  ✗ Tracking Column Consistency: {error}")
        
        passed_checks = sum(1 for c in checks if c["status"] == "PASS")
        total_checks = len([c for c in checks if c["status"] in ["PASS", "FAIL"]])
        table_quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        validation_id = f"{pipeline_run_id}_{table_name}_postrollback"
        validation_passed = len([c for c in checks if c["status"] == "FAIL"]) == 0
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
            validation_passed=str(validation_passed).lower(),
            notes_json=notes_json
        )
        spark.sql(insert_validation_query)
        
        validation_results.append({
            "table_name": table_name,
            "quality_score": round(table_quality_score, 2),
            "validation_passed": validation_passed,
            "checks": checks
        })
        
        print(f"  Quality Score: {table_quality_score:.2f}%")
    
    except Exception as e:
        error = f"Validation error for '{table_name}': {str(e)}"
        print(f"  ✗ {error}")
        validation_failures.append(error)

# ==========================================
# STEP 3: Create Post-Rollback Checkpoint
# ==========================================
print("\n" + "="*60)
print("STEP 3: Creating Post-Rollback Checkpoint")
print("="*60)

checkpoint_created = False

try:
    post_checkpoint_name = f"{checkpoint_name}_post_rollback"
    post_checkpoint_id = f"{pipeline_run_id}_post_checkpoint"
    retention_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    total_rows_restored = sum(s["total_rows"] for s in post_rollback_snapshots)
    notes = f"Post-rollback checkpoint after restoring to: {checkpoint_name}. {len(succeeded_tables)} tables restored."
    notes_escaped = notes.replace("'", "''")

    insert_checkpoint_query = """
        INSERT INTO metadata.CheckpointHistory (
            CheckpointId, CheckpointName, CheckpointType, CreatedDate,
            TablesIncluded, TotalRows, ValidationStatus, RetentionDate,
            IsActive, Notes, CreatedBy
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
            '{notes_escaped}',
            'System'
        )
    """.format(
        post_checkpoint_id=post_checkpoint_id,
        post_checkpoint_name=post_checkpoint_name,
        tables_included=len(succeeded_tables),
        total_rows=total_rows_restored,
        validation_status="Validated" if validation_passed else "ValidationFailed",
        retention_date=retention_date,
        notes_escaped=notes_escaped
    )
    spark.sql(insert_checkpoint_query)
    
    checkpoint_created = True
    print(f"✓ Post-rollback checkpoint created: {post_checkpoint_name}")
    print(f"  Tables: {len(succeeded_tables)}")
    print(f"  Total Rows: {total_rows_restored:,}")
    print(f"  Retention: 30 days (until {retention_date})")

except Exception as e:
    print(f"✗ Failed to create post-rollback checkpoint: {str(e)}")

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
    total_records_restored = 0
    total_records_removed = 0
    
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
    
    report_id = f"{pipeline_run_id}_comparison"
    issues_json = json.dumps(validation_failures).replace("'", "''")

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
            {validation_passed},
            '{issues_json}',
            'Rollback execution completed',
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
        quality_score=round(sum(v["quality_score"] for v in validation_results)/len(validation_results), 2) if validation_results else 0,
        validation_passed=str(validation_passed).lower(),
        issues_json=issues_json
    )
    spark.sql(insert_report_query)
    
    print(f"✓ Comparison report created")
    print(f"\n📊 DATA CHANGES:")
    print(f"  Records Restored: {total_records_restored:,}")
    print(f"  Records Removed: {total_records_removed:,}")
    print(f"  Net Change: {total_records_restored - total_records_removed:+,}")

except Exception as e:
    print(f"✗ Failed to generate comparison report: {str(e)}")

# ==========================================
# STEP 5: Log Final Summary
# ==========================================
print("\n" + "="*60)
print("STEP 5: Logging Final Summary")
print("="*60)

try:
    summary_notes = f"Validation: {'PASS' if validation_passed else 'FAIL'}. Quality Score: {round(sum(v['quality_score'] for v in validation_results)/len(validation_results),2) if validation_results else 0:.2f}%. Checkpoint Created: {checkpoint_created}"
    summary_notes_escaped = summary_notes.replace("'", "''")

    insert_log_query = """
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, RowsProcessed, Status, Notes, CreatedDate
        ) VALUES (
            '{log_id}',
            '{pipeline_run_id}',
            'ManualRollback',
            '{table_list}',
            'PostRollbackValidation',
            current_timestamp(),
            current_timestamp(),
            {rows_processed},
            '{status}',
            '{notes}',
            current_timestamp()
        )
    """.format(
        log_id=f"{pipeline_run_id}_postvalidation",
        pipeline_run_id=pipeline_run_id,
        table_list=",".join(succeeded_tables[:5]) + ("..." if len(succeeded_tables) > 5 else ""),
        rows_processed=sum(s["total_rows"] for s in post_rollback_snapshots),
        status="Success" if validation_passed else "Warning",
        notes=summary_notes_escaped
    )
    spark.sql(insert_log_query)
    
    print(f"✓ Summary logged successfully")

except Exception as e:
    print(f"✗ Failed to log summary: {str(e)}")

# ==========================================
# FINAL OUTPUT
# ==========================================
print("\n" + "="*60)
print("FINAL OUTPUT")
print("="*60)

output = {
    "validation_passed": validation_passed,
    "validation_failures": validation_failures,
    "quality_score": round(sum(v["quality_score"] for v in validation_results)/len(validation_results), 2) if validation_results else 0,
    "checkpoint_created": checkpoint_created,
    "comparison_report": table_comparisons if 'table_comparisons' in locals() else []
}

print(json.dumps(output, indent=2))
mssparkutils.notebook.exit(json.dumps(output))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
