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

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 1: FINDING CHECKPOINTS
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 1.1: Find all active checkpoints
# MAGIC SELECT 
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     TablesIncluded,
# MAGIC     TotalRows,
# MAGIC     ValidationStatus,
# MAGIC     RetentionDate,
# MAGIC     DATEDIFF(day, CreatedDate, GETDATE()) as days_old,
# MAGIC     DATEDIFF(day, GETDATE(), RetentionDate) as days_until_expiry,
# MAGIC     Notes
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE IsActive = true
# MAGIC ORDER BY CreatedDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1.2: Find checkpoints from a specific date range
# MAGIC SELECT 
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     ValidationStatus
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE CreatedDate BETWEEN '2025-01-10' AND '2025-01-20'
# MAGIC AND IsActive = true
# MAGIC ORDER BY CreatedDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1.3: Find the most recent checkpoint by type
# MAGIC SELECT TOP 1
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     ValidationStatus
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE CheckpointType = 'DailyBatch'
# MAGIC AND IsActive = true
# MAGIC ORDER BY CreatedDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1.4: Find all PrePurge checkpoints (for rollback after purge issues)
# MAGIC SELECT 
# MAGIC     CheckpointName,
# MAGIC     CreatedDate,
# MAGIC     TablesIncluded,
# MAGIC     TotalRows,
# MAGIC     ValidationStatus,
# MAGIC     Notes
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE CheckpointType = 'PrePurge'
# MAGIC AND IsActive = true
# MAGIC ORDER BY CreatedDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1.5: Check if specific checkpoint is restorable
# MAGIC SELECT 
# MAGIC     c.CheckpointName,
# MAGIC     c.CreatedDate,
# MAGIC     c.IsActive,
# MAGIC     c.ValidationStatus,
# MAGIC     c.RetentionDate,
# MAGIC     CASE 
# MAGIC         WHEN c.IsActive = false THEN 'EXPIRED'
# MAGIC         WHEN c.ValidationStatus != 'Validated' THEN 'NOT VALIDATED'
# MAGIC         WHEN GETDATE() > c.RetentionDate THEN 'PAST RETENTION DATE'
# MAGIC         ELSE 'OK TO RESTORE'
# MAGIC     END AS RestorabilityStatus
# MAGIC FROM metadata.CheckpointHistory c
# MAGIC WHERE c.CheckpointName = 'bronze_backup_2025-01-15';  -- Replace with your checkpoint

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1.6: Find checkpoints expiring soon (next 7 days)
# MAGIC SELECT 
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     RetentionDate,
# MAGIC     DATEDIFF(day, GETDATE(), RetentionDate) as days_until_expiry
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE IsActive = true
# MAGIC AND RetentionDate BETWEEN GETDATE() AND DATEADD(day, 7, GETDATE())
# MAGIC ORDER BY RetentionDate ASC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 2: ROLLBACK HISTORY & MONITORING
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 2.1: View all rollback operations (summary)
# MAGIC SELECT 
# MAGIC     r.ReportId,
# MAGIC     r.CheckpointName,
# MAGIC     r.RollbackDate,
# MAGIC     r.TablesAffected,
# MAGIC     r.TablesSucceeded,
# MAGIC     r.TablesFailed,
# MAGIC     r.RecordsRestored,
# MAGIC     r.RecordsRemoved,
# MAGIC     r.QualityScore,
# MAGIC     r.ValidationPassed,
# MAGIC     CASE 
# MAGIC         WHEN r.ValidationPassed = true AND r.TablesFailed = 0 THEN 'Complete Success'
# MAGIC         WHEN r.ValidationPassed = true AND r.TablesFailed > 0 THEN 'Partial Success'
# MAGIC         ELSE 'Failed'
# MAGIC     END AS OverallStatus
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC ORDER BY r.RollbackDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2.2: Rollback success metrics (last 30 days)
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rollbacks,
# MAGIC     SUM(CASE WHEN ValidationPassed = true THEN 1 ELSE 0 END) as successful,
# MAGIC     SUM(CASE WHEN ValidationPassed = false THEN 1 ELSE 0 END) as failed,
# MAGIC     CAST(AVG(QualityScore) AS DECIMAL(5,2)) as avg_quality_score,
# MAGIC     SUM(TablesAffected) as total_tables_processed,
# MAGIC     SUM(TablesSucceeded) as total_tables_succeeded,
# MAGIC     SUM(TablesFailed) as total_tables_failed,
# MAGIC     SUM(RecordsRestored) as total_records_restored,
# MAGIC     SUM(RecordsRemoved) as total_records_removed
# MAGIC FROM metadata.RollbackComparisonReports
# MAGIC WHERE RollbackDate >= DATEADD(day, -30, GETDATE());

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2.3: Find failed rollbacks
# MAGIC SELECT 
# MAGIC     r.ReportId,
# MAGIC     r.CheckpointName,
# MAGIC     r.RollbackDate,
# MAGIC     r.TablesAffected,
# MAGIC     r.TablesFailed,
# MAGIC     r.QualityScore,
# MAGIC     r.Issues,
# MAGIC     r.Notes
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC WHERE r.ValidationPassed = false
# MAGIC ORDER BY r.RollbackDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2.4: Detailed audit trail for specific rollback
# MAGIC -- Replace 'YOUR_PIPELINE_RUN_ID' with actual PipelineRunId
# MAGIC SELECT 
# MAGIC     Operation,
# MAGIC     TableName,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     DATEDIFF(second, StartTime, EndTime) as duration_seconds,
# MAGIC     RowsProcessed,
# MAGIC     Status,
# MAGIC     ErrorMessage,
# MAGIC     Notes
# MAGIC FROM metadata.SyncAuditLog
# MAGIC WHERE PipelineRunId = 'YOUR_PIPELINE_RUN_ID'
# MAGIC AND Operation IN ('RollbackValidation', 'RollbackImpactAnalysis', 'TableRestore', 'PostRollbackValidation')
# MAGIC ORDER BY StartTime;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2.5: Find all table restoration attempts (success and failed)
# MAGIC SELECT 
# MAGIC     PipelineRunId,
# MAGIC     TableName,
# MAGIC     StartTime,
# MAGIC     Status,
# MAGIC     ErrorMessage,
# MAGIC     DATEDIFF(second, StartTime, EndTime) as duration_seconds
# MAGIC FROM metadata.SyncAuditLog
# MAGIC WHERE Operation = 'TableRestore'
# MAGIC AND StartTime >= DATEADD(day, -7, GETDATE())
# MAGIC ORDER BY StartTime DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2.6: Rollback frequency by checkpoint type
# MAGIC SELECT 
# MAGIC     c.CheckpointType,
# MAGIC     COUNT(r.ReportId) as rollback_count,
# MAGIC     AVG(r.QualityScore) as avg_quality_score
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC INNER JOIN metadata.CheckpointHistory c ON r.CheckpointName = c.CheckpointName
# MAGIC WHERE r.RollbackDate >= DATEADD(month, -1, GETDATE())
# MAGIC GROUP BY c.CheckpointType
# MAGIC ORDER BY rollback_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
