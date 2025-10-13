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
# STEP 3: Aggregate Quality Score
# ==========================================
print("\n" + "="*60)
print("STEP 3: Aggregating Quality Scores")
print("="*60)

total_quality_score = sum([r["quality_score"] for r in validation_results])
num_tables = len(validation_results)
overall_quality_score = round(total_quality_score / num_tables, 2) if num_tables > 0 else 0

print(f"\nOverall Quality Score: {overall_quality_score:.2f}%")
print(f"Tables Passed: {sum(1 for r in validation_results if r['validation_passed'])}/{num_tables}")
print(f"Tables Failed: {len(validation_failures)}")

# ==========================================
# STEP 4: Log Aggregate Results
# ==========================================
print("\n" + "="*60)
print("STEP 4: Logging Aggregate Results")
print("="*60)

try:
    summary_notes = json.dumps({
        "validation_failures": validation_failures[:10],
        "total_tables": num_tables,
        "overall_quality_score": overall_quality_score
    }).replace("'", "''")
    
    spark.sql("""
        INSERT INTO metadata.DataValidationSummary (
            SummaryId, PipelineRunId, ValidationDate,
            OverallQualityScore, TotalTables, Notes
        ) VALUES (
            '{summary_id}',
            '{pipeline_run_id}',
            current_timestamp(),
            {overall_quality_score},
            {num_tables},
            '{summary_notes}'
        )
    """.format(
        summary_id=f"{pipeline_run_id}_summary",
        pipeline_run_id=pipeline_run_id,
        overall_quality_score=overall_quality_score,
        num_tables=num_tables,
        summary_notes=summary_notes
    ))
    
    print(f"✓ Aggregate validation results logged")
except Exception as e:
    print(f"⚠ Failed to log summary: {str(e)}")

# ==========================================
# STEP 5: Return Final Results
# ==========================================
print("\n" + "="*60)
print("POST-ROLLBACK VALIDATION COMPLETE")
print("="*60)

result = {
    "validation_passed": len(validation_failures) == 0,
    "checkpoint_info": {
        "id": checkpoint_id,
        "name": checkpoint_name
    },
    "quality_score": overall_quality_score,
    "tables_validated": validation_results,
    "failed_tables": validation_failures
}

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
