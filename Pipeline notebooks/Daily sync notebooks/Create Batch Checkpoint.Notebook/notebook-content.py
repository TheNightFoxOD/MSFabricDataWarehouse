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

pipeline_run_id = "pipeline_run_id_default"
pipeline_trigger_time = "pipeline_trigger_time_default"
batch_checkpoint_retention_days = 7

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# CELL 1: Import Libraries and Display Parameters
# ==============================================================================

import json
from datetime import datetime, timedelta
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DateType, BooleanType

# Parameters are already set by pipeline in the parameters cell above
# If running manually for testing, they'll use the default values

print("="*80)
print("CREATE BATCH CHECKPOINT")
print("="*80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Pipeline Trigger Time: {pipeline_trigger_time}")
print(f"Retention Period: {batch_checkpoint_retention_days} days")
print("="*80)

# For testing: use test values if parameters are empty
if not pipeline_run_id:
    print("\n⚠️  No pipeline_run_id provided - using test value")
    pipeline_run_id = "test-run-id"
    pipeline_trigger_time = datetime.utcnow().isoformat()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# CELL 2: Query Per-Table Checkpoints from This Run
# ==============================================================================

# Query all per-table checkpoints created during this pipeline run
per_table_query = f"""
    SELECT 
        COUNT(DISTINCT TableName) as tables_included,
        SUM(TotalRows) as total_rows,
        MIN(CreatedDate) as earliest_checkpoint,
        MAX(CreatedDate) as latest_checkpoint,
        SUM(CASE WHEN ValidationStatus = 'Validated' THEN 1 ELSE 0 END) as validated_count,
        COUNT(*) as total_checkpoints
    FROM metadata.CheckpointHistory
    WHERE PipelineRunId = '{pipeline_run_id}'
    AND CheckpointType = 'Daily'
"""

# Flag to control execution of later cells
should_create_checkpoint = False
tables_included = 0
total_rows = 0
earliest_checkpoint = None
latest_checkpoint = None
validated_count = 0
total_checkpoints = 0

try:
    per_table_stats = spark.sql(per_table_query).collect()[0]
    
    tables_included = per_table_stats['tables_included'] or 0
    total_rows = per_table_stats['total_rows'] or 0
    earliest_checkpoint = per_table_stats['earliest_checkpoint']
    latest_checkpoint = per_table_stats['latest_checkpoint']
    validated_count = per_table_stats['validated_count'] or 0
    total_checkpoints = per_table_stats['total_checkpoints'] or 0
    
    print(f"\n📊 Per-Table Checkpoint Summary:")
    print(f"   Tables Processed: {tables_included}")
    print(f"   Total Checkpoints: {total_checkpoints}")
    print(f"   Validated Checkpoints: {validated_count}")
    print(f"   Total Rows: {total_rows:,}")
    if earliest_checkpoint:
        print(f"   Earliest Checkpoint: {earliest_checkpoint}")
        print(f"   Latest Checkpoint: {latest_checkpoint}")
        
        # Calculate time span
        if earliest_checkpoint and latest_checkpoint:
            time_span = latest_checkpoint - earliest_checkpoint
            print(f"   Processing Time Span: {time_span}")
    
    # Determine if we should create batch checkpoint
    should_create_checkpoint = (
        tables_included > 0 and 
        total_rows > 0 and
        validated_count == total_checkpoints  # All per-table checkpoints validated
    )
    
    if not should_create_checkpoint:
        reasons = []
        if tables_included == 0:
            reasons.append("no tables processed")
        if total_rows == 0:
            reasons.append("no rows processed")
        if validated_count != total_checkpoints:
            reasons.append(f"only {validated_count}/{total_checkpoints} checkpoints validated")
        
        print(f"\n⏭️  Skipping batch checkpoint creation: {', '.join(reasons)}")
        print("   This is normal if no tables were synced in this pipeline run")
        print("\n✅ Notebook execution complete - no batch checkpoint needed")
    
except Exception as e:
    error_msg = f"Failed to query per-table checkpoints: {str(e)}"
    print(f"\n❌ {error_msg}")
    print(f"   Query used: {per_table_query}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# CELL 3: Create Batch Checkpoint Entry
# ==============================================================================

if not should_create_checkpoint:
    print("\n⏭️  Skipping batch checkpoint creation (cell 3)")
else:
    from delta.tables import DeltaTable
    
    checkpoint_id = str(uuid.uuid4())
    
    # ========================================
    # Use ACTUAL Delta commit timestamps, not wall-clock time
    # ========================================
    
    print(f"\n🔍 Finding actual Delta commit timestamps for checkpoint...")
    print("="*80)
    
    # Query all tables that were processed in this run
    tables_query = f"""
        SELECT DISTINCT SchemaName, TableName
        FROM metadata.CheckpointHistory
        WHERE PipelineRunId = '{pipeline_run_id}'
        AND CheckpointType = 'Daily'
    """
    
    tables_in_run = spark.sql(tables_query).collect()
    
    # Get the latest Delta commit timestamp across all tables
    latest_delta_commit = None
    earliest_delta_commit = None
    table_timestamps = []
    
    for table_row in tables_in_run:
        schema_name = table_row.SchemaName
        table_name = table_row.TableName
        full_table_name = f"{schema_name}.{table_name}"
        
        try:
            # Get the latest Delta commit timestamp for this table
            delta_table = DeltaTable.forName(spark, full_table_name)
            history = delta_table.history(1)  # Get only latest commit
            
            if history.count() > 0:
                latest_commit = history.collect()[0]
                commit_timestamp = latest_commit.timestamp
                
                table_timestamps.append({
                    "table": full_table_name,
                    "timestamp": commit_timestamp,
                    "version": latest_commit.version
                })
                
                print(f"   {full_table_name}: version {latest_commit.version} at {commit_timestamp}")
                
                # Track earliest and latest across all tables
                if latest_delta_commit is None or commit_timestamp > latest_delta_commit:
                    latest_delta_commit = commit_timestamp
                
                if earliest_delta_commit is None or commit_timestamp < earliest_delta_commit:
                    earliest_delta_commit = commit_timestamp
        
        except Exception as e:
            print(f"   ⚠️  Warning: Could not get Delta timestamp for {full_table_name}: {str(e)}")
            continue
    
    # Use the LATEST Delta commit timestamp as the checkpoint timestamp
    # This ensures the checkpoint timestamp is never AFTER the last actual data change
    if latest_delta_commit is None:
        print("\n❌ ERROR: Could not determine Delta commit timestamps for any tables")
        print("   Falling back to current time (this may cause rollback issues)")
        checkpoint_timestamp = datetime.utcnow()
    else:
        checkpoint_timestamp = latest_delta_commit
        print(f"\n✅ Checkpoint timestamp determined from actual Delta commits:")
        print(f"   Earliest table commit: {earliest_delta_commit}")
        print(f"   Latest table commit: {latest_delta_commit}")
        print(f"   Using: {checkpoint_timestamp} (latest commit)")
    
    checkpoint_name = f"bronze_backup_batch_{checkpoint_timestamp.strftime('%Y-%m-%d_%H%M%S')}"
    
    # Use configurable retention period
    retention_days = int(batch_checkpoint_retention_days)
    retention_date = (checkpoint_timestamp + timedelta(days=retention_days)).date()
    
    print(f"\n🔄 Creating Batch Checkpoint:")
    print("="*80)
    print(f"   Checkpoint ID: {checkpoint_id}")
    print(f"   Checkpoint Name: {checkpoint_name}")
    print(f"   Tables Included: {tables_included}")
    print(f"   Total Rows: {total_rows:,}")
    print(f"   Checkpoint Timestamp: {checkpoint_timestamp} (from actual Delta commits)")
    print(f"   Retention Period: {retention_days} days")
    print(f"   Retention Date: {retention_date}")
    print("="*80)
    
    # Define schema matching CheckpointHistory table
    checkpoint_schema = StructType([
        StructField("CheckpointId", StringType(), False),
        StructField("CheckpointName", StringType(), False),
        StructField("CheckpointType", StringType(), False),
        StructField("CreatedDate", TimestampType(), False),
        StructField("TablesIncluded", IntegerType(), False),
        StructField("TotalRows", LongType(), True),
        StructField("ValidationStatus", StringType(), False),
        StructField("RetentionDate", DateType(), False),
        StructField("IsActive", BooleanType(), False),
        StructField("PipelineRunId", StringType(), True),
        StructField("SchemaName", StringType(), True),
        StructField("TableName", StringType(), True),
    ])
    
    try:
        # Create batch checkpoint entry with CORRECT timestamp
        batch_checkpoint = [(
            checkpoint_id,
            checkpoint_name,
            'DailyBatch',
            checkpoint_timestamp,  # ← Now uses actual Delta commit time, not wall-clock time
            int(tables_included),
            int(total_rows),
            'Validated',
            retention_date,
            True,
            pipeline_run_id,
            None,  # SchemaName: NULL for batch checkpoints
            None   # TableName: NULL for batch checkpoints
        )]
        
        checkpoint_df = spark.createDataFrame(batch_checkpoint, checkpoint_schema)
        checkpoint_df.write.format("delta").mode("append").saveAsTable("metadata.CheckpointHistory")
        
        print(f"\n✅ Batch checkpoint created successfully!")
        print(f"   Checkpoint represents synchronized restore point for {tables_included} tables")
        print(f"   Aggregated {total_rows:,} total rows across all tables")
        print(f"   This checkpoint is the PRIMARY target for rollback operations")
        print(f"   Retention: {retention_days} days (expires {retention_date})")
        
        print(f"\n📋 Checkpoint Details:")
        print(f"   ID: {checkpoint_id}")
        print(f"   Name: {checkpoint_name}")
        print(f"   Timestamp: {checkpoint_timestamp} (from actual Delta commits)")
        print(f"   Tables: {tables_included}")
        print(f"   Rows: {total_rows:,}")
        
        # Also log the per-table timestamps for audit trail
        print(f"\n📊 Per-Table Delta Timestamps:")
        for table_info in sorted(table_timestamps, key=lambda x: x['timestamp']):
            print(f"   {table_info['table']}: v{table_info['version']} @ {table_info['timestamp']}")
        
    except Exception as e:
        error_msg = f"Failed to create batch checkpoint: {str(e)}"
        print(f"\n❌ {error_msg}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# CELL 4: Verification Query (Conditional)
# ==============================================================================

if not should_create_checkpoint:
    print("\n⏭️  Skipping verification (cell 4) - no checkpoint was created")
    print("\n" + "="*80)
    print("Notebook complete - no batch checkpoint needed for this run")
    print("="*80)
else:
    # Query to verify batch checkpoint was created and linked correctly
    verification_query = f"""
        SELECT 
            CheckpointType,
            CheckpointName,
            TablesIncluded,
            TotalRows,
            ValidationStatus,
            CreatedDate,
            RetentionDate,
            IsActive
        FROM metadata.CheckpointHistory
        WHERE PipelineRunId = '{pipeline_run_id}'
        ORDER BY CheckpointType DESC, CreatedDate
    """
    
    try:
        print(f"\n📊 Verification - All Checkpoints from Pipeline Run {pipeline_run_id}:")
        print("="*80)
        verification_df = spark.sql(verification_query)
        verification_df.show(truncate=False)
        
        # Count by type
        type_counts = verification_df.groupBy("CheckpointType").count().collect()
        print("\nCheckpoint Type Summary:")
        for row in type_counts:
            print(f"   {row['CheckpointType']}: {row['count']}")
        
        print("\n" + "="*80)
        print("✅ Batch checkpoint creation complete!")
        print("="*80)
        
    except Exception as e:
        print(f"\n⚠️  Warning: Could not run verification query: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
