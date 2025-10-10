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

dry_run = "false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime, date

# Get parameters (optional - for manual execution with custom settings)
dry_run_mode = False

try:
    dry_run_param = dbutils.widgets.get("dry_run")
    dry_run_mode = dry_run_param.lower() == "true"
except:
    print("ℹ️  No dry_run parameter provided, defaulting to False (will make changes)")

print("="*80)
print("ENFORCE CHECKPOINT RETENTION POLICY")
print("="*80)
print(f"Dry Run Mode: {dry_run_mode}")
print(f"Current Date: {date.today()}")
print("="*80)

if dry_run_mode:
    print("\n⚠️  DRY RUN MODE ENABLED")
    print("   No changes will be made to the database")
    print("   This will only preview what would be deactivated\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 Query to find expired checkpoints that should be deactivated
# Excludes PrePurge and Manual checkpoint types from automatic deactivation
expired_query = """
    SELECT 
        CheckpointId,
        CheckpointName,
        CheckpointType,
        CreatedDate,
        RetentionDate,
        DATEDIFF(CURRENT_DATE(), RetentionDate) as days_past_retention,
        TablesIncluded,
        TotalRows,
        SchemaName,
        TableName,
        PipelineRunId
    FROM metadata.CheckpointHistory
    WHERE IsActive = true
    AND RetentionDate < CURRENT_DATE()
    AND CheckpointType NOT IN ('PrePurge', 'Manual')
    ORDER BY RetentionDate
"""

try:
    expired_df = spark.sql(expired_query)
    expired_count = expired_df.count()
    
    if expired_count == 0:
        print("\n✅ No expired checkpoints found")
        print("   All active checkpoints are within their retention period")
        print("   No action needed")
        
        dbutils.notebook.exit(json.dumps({
            "status": "success",
            "expired_count": 0,
            "deactivated_count": 0,
            "dry_run": dry_run_mode,
            "message": "No expired checkpoints found"
        }))
    
    print(f"\n📊 Found {expired_count} expired checkpoint(s) eligible for deactivation:")
    print("="*80)
    
    # Group by checkpoint type for summary
    summary = expired_df.groupBy("CheckpointType").count().collect()
    for row in summary:
        print(f"   {row['CheckpointType']}: {row['count']} checkpoint(s)")
    
    # Show details of expired checkpoints
    print("\n📋 Expired Checkpoint Details:")
    print("="*80)
    expired_list = expired_df.collect()
    
    display_limit = min(20, expired_count)  # Show up to 20 checkpoints
    
    for i, checkpoint in enumerate(expired_list[:display_limit], 1):
        table_info = f"{checkpoint['SchemaName']}.{checkpoint['TableName']}" if checkpoint['TableName'] else "ALL TABLES"
        
        print(f"\n{i}. {checkpoint['CheckpointName']}")
        print(f"   Type: {checkpoint['CheckpointType']}")
        print(f"   Table: {table_info}")
        print(f"   Created: {checkpoint['CreatedDate']}")
        print(f"   Retention Date: {checkpoint['RetentionDate']}")
        print(f"   Days Overdue: {checkpoint['days_past_retention']}")
        if checkpoint['TotalRows']:
            print(f"   Total Rows: {checkpoint['TotalRows']:,}")
    
    if expired_count > display_limit:
        print(f"\n   ... and {expired_count - display_limit} more checkpoint(s)")
    
    # Store checkpoint IDs for deactivation
    checkpoint_ids = [row['CheckpointId'] for row in expired_list]
    
    print(f"\n{'='*80}")
    print(f"Total checkpoints to deactivate: {expired_count}")
    print(f"{'='*80}")
    
except Exception as e:
    error_msg = f"Failed to query expired checkpoints: {str(e)}"
    print(f"\n❌ {error_msg}")
    
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "error_message": error_msg
    }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if dry_run_mode:
    print(f"\n🔍 DRY RUN: Would deactivate {expired_count} checkpoint(s)")
    print("   No changes made to database")
    print("\n   To actually deactivate these checkpoints, run with dry_run = false")
    
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "expired_count": expired_count,
        "deactivated_count": 0,
        "dry_run": True,
        "message": f"Dry run completed - {expired_count} checkpoints would be deactivated"
    }))

# Perform actual deactivation
print(f"\n🔄 Deactivating {expired_count} expired checkpoint(s)...")

try:
    # Build update statement with checkpoint IDs
    # Using SQL UPDATE with IN clause
    checkpoint_ids_str = "','".join(checkpoint_ids)
    
    update_sql = f"""
        UPDATE metadata.CheckpointHistory
        SET IsActive = false
        WHERE CheckpointId IN ('{checkpoint_ids_str}')
    """
    
    spark.sql(update_sql)
    
    print(f"\n✅ Successfully deactivated {expired_count} expired checkpoint(s)")
    print("   Checkpoint metadata preserved for audit history")
    print("   These checkpoints are now marked IsActive = false")
    
    # Verify deactivation
    verify_query = f"""
        SELECT COUNT(*) as deactivated_count
        FROM metadata.CheckpointHistory
        WHERE CheckpointId IN ('{checkpoint_ids_str}')
        AND IsActive = false
    """
    
    deactivated_count = spark.sql(verify_query).collect()[0]['deactivated_count']
    
    if deactivated_count == expired_count:
        print(f"\n✅ Verification passed: All {deactivated_count} checkpoint(s) confirmed inactive")
    else:
        print(f"\n⚠️  Verification warning: Expected {expired_count}, found {deactivated_count} inactive")
        print("   Some checkpoints may not have been deactivated")
    
    # Return success
    result = {
        "status": "success",
        "expired_count": expired_count,
        "deactivated_count": deactivated_count,
        "dry_run": False,
        "message": f"Successfully deactivated {deactivated_count} expired checkpoints"
    }
    
    print(f"\n📋 Notebook Result:")
    print(json.dumps(result, indent=2))
    
    dbutils.notebook.exit(json.dumps(result))
    
except Exception as e:
    error_msg = f"Failed to deactivate expired checkpoints: {str(e)}"
    print(f"\n❌ {error_msg}")
    
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "error_message": error_msg,
        "expired_count": expired_count,
        "deactivated_count": 0
    }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Query final checkpoint statistics after retention enforcement
stats_query = """
    SELECT 
        CheckpointType,
        COUNT(*) as total_count,
        SUM(CASE WHEN IsActive = true THEN 1 ELSE 0 END) as active_count,
        SUM(CASE WHEN IsActive = false THEN 1 ELSE 0 END) as inactive_count,
        MIN(RetentionDate) as earliest_retention,
        MAX(RetentionDate) as latest_retention
    FROM metadata.CheckpointHistory
    GROUP BY CheckpointType
    ORDER BY CheckpointType
"""

try:
    print(f"\n📊 Checkpoint Statistics After Retention Enforcement:")
    print("="*80)
    
    stats_df = spark.sql(stats_query)
    stats_df.show(truncate=False)
    
    # Additional summary
    total_stats = spark.sql("""
        SELECT 
            COUNT(*) as total_checkpoints,
            SUM(CASE WHEN IsActive = true THEN 1 ELSE 0 END) as active_checkpoints,
            SUM(CASE WHEN IsActive = false THEN 1 ELSE 0 END) as inactive_checkpoints
        FROM metadata.CheckpointHistory
    """).collect()[0]
    
    print(f"\nOverall Summary:")
    print(f"   Total Checkpoints (all time): {total_stats['total_checkpoints']}")
    print(f"   Active Checkpoints: {total_stats['active_checkpoints']}")
    print(f"   Inactive Checkpoints: {total_stats['inactive_checkpoints']}")
    
    # Check for checkpoints expiring soon (next 7 days)
    expiring_soon_query = """
        SELECT COUNT(*) as expiring_soon_count
        FROM metadata.CheckpointHistory
        WHERE IsActive = true
        AND RetentionDate BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), 7)
        AND CheckpointType NOT IN ('PrePurge', 'Manual')
    """
    
    expiring_soon = spark.sql(expiring_soon_query).collect()[0]['expiring_soon_count']
    
    if expiring_soon > 0:
        print(f"\n⏰ Warning: {expiring_soon} checkpoint(s) will expire in the next 7 days")
        print("   Consider scheduling the next retention enforcement run")
    else:
        print(f"\n✅ No checkpoints expiring in the next 7 days")
    
    print("="*80)
    
except Exception as e:
    print(f"\n⚠️  Warning: Could not retrieve statistics: {str(e)}")
    print("   Deactivation was successful, but statistics query failed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
