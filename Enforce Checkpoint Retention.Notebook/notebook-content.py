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

# ==============================================================================
# CELL 1: Import Libraries and Process Parameters
# ==============================================================================

import json
from datetime import datetime, date

# Convert string parameter to boolean
dry_run_mode = dry_run.lower() == "true"

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

# ==============================================================================
# CELL 2: Identify Expired Checkpoints
# ==============================================================================

# Query to find expired checkpoints that should be deactivated
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

expired_df = spark.sql(expired_query)
expired_count = expired_df.count()

# Flag to control execution of later cells
should_deactivate = False

if expired_count == 0:
    print("\n✅ No expired checkpoints found")
    print("   All active checkpoints are within their retention period")
    print("   No action needed")
    print("\nNotebook execution complete - no changes made")
else:
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
    
    display_limit = min(20, expired_count)
    
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
    
    # Set flag to proceed with deactivation (unless dry run)
    should_deactivate = not dry_run_mode

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# CELL 3: Deactivate Expired Checkpoints (Conditional Execution)
# ==============================================================================

if expired_count == 0:
    print("\nSkipping deactivation - no expired checkpoints found")
elif dry_run_mode:
    print(f"\n🔍 DRY RUN: Would deactivate {expired_count} checkpoint(s)")
    print("   No changes made to database")
    print("\n   To actually deactivate these checkpoints, run with dry_run = false")
    print("\nNotebook execution complete - dry run mode")
elif should_deactivate:
    # Perform actual deactivation
    print(f"\n🔄 Deactivating {expired_count} expired checkpoint(s)...")
    
    # Build update statement with checkpoint IDs
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
    
    print(f"\nDeactivated {deactivated_count} of {expired_count} expired checkpoints")
else:
    print("\nSkipping deactivation - conditions not met")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# CELL 4: Summary Statistics
# ==============================================================================

# Only show statistics if we actually deactivated something
if should_deactivate and expired_count > 0:
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
    print("Retention enforcement complete!")
    print("="*80)
else:
    print("\nNo statistics to display - no deactivations performed")
    print("Notebook execution complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
