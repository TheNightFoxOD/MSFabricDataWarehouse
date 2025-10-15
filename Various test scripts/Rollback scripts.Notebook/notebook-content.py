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
# MAGIC -- 1.1: Find all active checkpoints (WITH CheckpointId for rollback pipeline)
# MAGIC SELECT
# MAGIC     CheckpointId,        -- Use this for pipeline parameter!
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     TablesIncluded,
# MAGIC     TotalRows,
# MAGIC     ValidationStatus,
# MAGIC     RetentionDate,
# MAGIC     DATEDIFF(CURRENT_DATE(), CAST(CreatedDate AS DATE)) as days_old,
# MAGIC     DATEDIFF(CAST(RetentionDate AS DATE), CURRENT_DATE()) as days_until_expiry
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
# MAGIC -- 1.2: Find checkpoints from a specific date range (USE CheckpointId for pipeline!)
# MAGIC SELECT
# MAGIC     CheckpointId,        -- Pipeline parameter value
# MAGIC     CheckpointName,      -- For readability
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
# MAGIC -- 1.3: Find the most recent checkpoint by type (USE CheckpointId!)
# MAGIC SELECT
# MAGIC     CheckpointId,        -- This is what you pass to the pipeline
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     ValidationStatus,
# MAGIC     IsActive
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE CheckpointType = 'DailyBatch'
# MAGIC AND IsActive = true
# MAGIC ORDER BY CreatedDate DESC
# MAGIC -- LIMIT 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1.4: Find all PrePurge checkpoints (for rollback after purge issues)
# MAGIC SELECT
# MAGIC     CheckpointId,
# MAGIC     CheckpointName,
# MAGIC     CreatedDate,
# MAGIC     TablesIncluded,
# MAGIC     TotalRows,
# MAGIC     ValidationStatus,
# MAGIC     RetentionDate
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
# MAGIC     c.CheckpointId,
# MAGIC     c.CheckpointName,
# MAGIC     c.CreatedDate,
# MAGIC     c.IsActive,
# MAGIC     c.ValidationStatus,
# MAGIC     c.RetentionDate,
# MAGIC     CASE
# MAGIC         WHEN c.IsActive = false THEN 'EXPIRED'
# MAGIC         WHEN c.ValidationStatus != 'Validated' THEN 'NOT VALIDATED'
# MAGIC         WHEN CURRENT_TIMESTAMP() > c.RetentionDate THEN 'PAST RETENTION DATE'
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
# MAGIC     CheckpointId,
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     RetentionDate,
# MAGIC     DATEDIFF(CAST(RetentionDate AS DATE), CURRENT_DATE()) as days_until_expiry
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE IsActive = true
# MAGIC AND CAST(RetentionDate AS DATE) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), 7)
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
# MAGIC -- 2.1: View all rollback operations (summary) with checkpoint details
# MAGIC SELECT
# MAGIC     r.ReportId,
# MAGIC     r.CheckpointId,      -- FK to CheckpointHistory
# MAGIC     r.CheckpointName,    -- For readability
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
# MAGIC -- 2.1b: Join with CheckpointHistory to get full checkpoint details
# MAGIC SELECT
# MAGIC     r.RollbackDate,
# MAGIC     r.CheckpointName,
# MAGIC     c.CheckpointType,
# MAGIC     c.CreatedDate as CheckpointCreatedDate,
# MAGIC     r.TablesAffected,
# MAGIC     r.TablesSucceeded,
# MAGIC     r.QualityScore,
# MAGIC     r.ValidationPassed
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC INNER JOIN metadata.CheckpointHistory c ON r.CheckpointId = c.CheckpointId
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
# MAGIC WHERE RollbackDate >= DATE_SUB(CURRENT_DATE(), 30);

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
# MAGIC     CAST((UNIX_TIMESTAMP(EndTime) - UNIX_TIMESTAMP(StartTime)) AS INT) as duration_seconds,
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
# MAGIC     CAST((UNIX_TIMESTAMP(EndTime) - UNIX_TIMESTAMP(StartTime)) AS INT) as duration_seconds
# MAGIC FROM metadata.SyncAuditLog
# MAGIC WHERE Operation = 'TableRestore'
# MAGIC AND StartTime >= DATE_SUB(CURRENT_DATE(), 7)
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
# MAGIC WHERE r.RollbackDate >= ADD_MONTHS(CURRENT_DATE(), -1)
# MAGIC GROUP BY c.CheckpointType
# MAGIC ORDER BY rollback_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 3: TROUBLESHOOTING QUERIES
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 3.1: Check Delta Lake time travel availability for a table
# MAGIC -- Run DESCRIBE HISTORY in a notebook, this shows retention info:
# MAGIC -- DESCRIBE HISTORY account;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# MAGIC %%sql
# MAGIC -- 3.2: Compare pre/post rollback states for a table
# MAGIC SELECT
# MAGIC     s.TableName,
# MAGIC     s.SnapshotType,
# MAGIC     s.SnapshotDate,
# MAGIC     s.TotalRows,
# MAGIC     s.ActiveRows,
# MAGIC     s.DeletedRows,
# MAGIC     s.PurgedRows,
# MAGIC     s.DeltaVersion
# MAGIC FROM metadata.RollbackStateSnapshots s
# MAGIC WHERE s.PipelineRunId = '073d5237-1469-4e34-a48d-a876c91b1719'
# MAGIC AND s.TableName = 'od_donation'  -- Replace with table of interest
# MAGIC ORDER BY s.SnapshotType;  -- PreRollback first, then PostRollback

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 3.3: Calculate data deltas from rollback
# MAGIC WITH PrePost AS (
# MAGIC     SELECT
# MAGIC         TableName,
# MAGIC         MAX(CASE WHEN SnapshotType = 'PreRollback' THEN TotalRows END) as pre_total,
# MAGIC         MAX(CASE WHEN SnapshotType = 'PostRollback' THEN TotalRows END) as post_total,
# MAGIC         MAX(CASE WHEN SnapshotType = 'PreRollback' THEN ActiveRows END) as pre_active,
# MAGIC         MAX(CASE WHEN SnapshotType = 'PostRollback' THEN ActiveRows END) as post_active
# MAGIC     FROM metadata.RollbackStateSnapshots
# MAGIC     WHERE PipelineRunId = 'YOUR_PIPELINE_RUN_ID'
# MAGIC     GROUP BY TableName
# MAGIC )
# MAGIC SELECT
# MAGIC     TableName,
# MAGIC     pre_total,
# MAGIC     post_total,
# MAGIC     (post_total - pre_total) as total_delta,
# MAGIC     pre_active,
# MAGIC     post_active,
# MAGIC     (post_active - pre_active) as active_delta,
# MAGIC     CAST(((post_total - pre_total) * 100.0 / NULLIF(pre_total, 0)) AS DECIMAL(10,2)) as percent_change
# MAGIC FROM PrePost
# MAGIC ORDER BY ABS(post_total - pre_total) DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 3.4: Find validation failures from recent rollbacks
# MAGIC SELECT
# MAGIC     r.RollbackDate,
# MAGIC     r.CheckpointName,
# MAGIC     r.QualityScore,
# MAGIC     r.Issues
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC WHERE r.ValidationPassed = false
# MAGIC AND r.RollbackDate >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC ORDER BY r.RollbackDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 3.5: Check for rollback attempts (including dry-runs)
# MAGIC SELECT
# MAGIC     PipelineRunId,
# MAGIC     Operation,
# MAGIC     StartTime,
# MAGIC     Status,
# MAGIC     Notes
# MAGIC FROM metadata.SyncAuditLog
# MAGIC WHERE Operation LIKE 'Rollback%'
# MAGIC AND StartTime >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC ORDER BY StartTime DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 3.6: Find tables that failed restoration
# MAGIC SELECT
# MAGIC     PipelineRunId,
# MAGIC     TableName,
# MAGIC     StartTime,
# MAGIC     ErrorMessage
# MAGIC FROM metadata.SyncAuditLog
# MAGIC WHERE Operation = 'TableRestore'
# MAGIC AND Status = 'Error'
# MAGIC AND StartTime >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC ORDER BY StartTime DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 4: DATA VALIDATION QUERIES
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 4.1: Check current data integrity for a table
# MAGIC SELECT
# MAGIC     'account' as TableName,  -- Replace with table name
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN IsDeleted = false AND IsPurged = false THEN 1 ELSE 0 END) as active_rows,
# MAGIC     SUM(CASE WHEN IsDeleted = true THEN 1 ELSE 0 END) as deleted_rows,
# MAGIC     SUM(CASE WHEN IsPurged = true THEN 1 ELSE 0 END) as purged_rows,
# MAGIC     -- Validation check
# MAGIC     CASE
# MAGIC         WHEN COUNT(*) =
# MAGIC             SUM(CASE WHEN IsDeleted = false AND IsPurged = false THEN 1 ELSE 0 END) +
# MAGIC             SUM(CASE WHEN IsDeleted = true THEN 1 ELSE 0 END) +
# MAGIC             SUM(CASE WHEN IsPurged = true THEN 1 ELSE 0 END)
# MAGIC         THEN 'VALID'
# MAGIC         ELSE 'INVALID - MISMATCH'
# MAGIC     END as validation_status
# MAGIC FROM account;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 4.2: Check for null primary keys across tables
# MAGIC -- Replace 'accountid' with actual primary key column
# MAGIC SELECT
# MAGIC     'account' as TableName,
# MAGIC     COUNT(*) as total_records,
# MAGIC     SUM(CASE WHEN accountid IS NULL THEN 1 ELSE 0 END) as null_pk_count
# MAGIC FROM account
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'donation',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN donationid IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM donation;
# MAGIC -- Add more tables as needed

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 4.3: Find records with inconsistent tracking flags
# MAGIC SELECT
# MAGIC     'account' as TableName,
# MAGIC     COUNT(*) as inconsistent_records
# MAGIC FROM account
# MAGIC WHERE IsDeleted = true AND IsPurged = true  -- Both flags shouldn't be true
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'donation',
# MAGIC     COUNT(*)
# MAGIC FROM donation
# MAGIC WHERE IsDeleted = true AND IsPurged = true;
# MAGIC -- Add more tables as needed

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 4.4: Recent validation results
# MAGIC SELECT
# MAGIC     ValidationDate,
# MAGIC     TableName,
# MAGIC     BronzeRowCount,
# MAGIC     ActiveRowCount,
# MAGIC     DeletedRowCount,
# MAGIC     PurgedRowCount,
# MAGIC     ValidationPassed,
# MAGIC     ValidationContext
# MAGIC FROM metadata.DataValidation
# MAGIC WHERE ValidationContext = 'PostRollback'
# MAGIC AND ValidationDate >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC ORDER BY ValidationDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 5: CHECKPOINT MANAGEMENT
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 5.1: Count checkpoints by type and status
# MAGIC SELECT
# MAGIC     CheckpointType,
# MAGIC     IsActive,
# MAGIC     COUNT(*) as checkpoint_count,
# MAGIC     MIN(CreatedDate) as oldest,
# MAGIC     MAX(CreatedDate) as newest
# MAGIC FROM metadata.CheckpointHistory
# MAGIC GROUP BY CheckpointType, IsActive
# MAGIC ORDER BY CheckpointType, IsActive DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 5.2: Find checkpoints created on specific date
# MAGIC SELECT
# MAGIC     CheckpointId,
# MAGIC     CheckpointName,
# MAGIC     CheckpointType,
# MAGIC     CreatedDate,
# MAGIC     TablesIncluded,
# MAGIC     ValidationStatus
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE CAST(CreatedDate AS DATE) = '2025-01-15'  -- Replace with your date
# MAGIC ORDER BY CreatedDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 5.3: Find post-rollback checkpoints
# MAGIC SELECT
# MAGIC     CheckpointId,
# MAGIC     CheckpointName,
# MAGIC     CreatedDate,
# MAGIC     TablesIncluded,
# MAGIC     TotalRows,
# MAGIC     ValidationStatus,
# MAGIC     RetentionDate
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE CheckpointType = 'PostRollback'
# MAGIC ORDER BY CreatedDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 5.4: Checkpoint storage analysis
# MAGIC SELECT
# MAGIC     CheckpointType,
# MAGIC     COUNT(*) as checkpoint_count,
# MAGIC     SUM(TotalRows) as total_rows_tracked,
# MAGIC     AVG(TablesIncluded) as avg_tables_per_checkpoint
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE IsActive = true
# MAGIC GROUP BY CheckpointType;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 6: REPORTING QUERIES
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 6.1: Monthly rollback activity report
# MAGIC SELECT
# MAGIC     DATE_FORMAT(r.RollbackDate, 'yyyy-MM') as rollback_month,
# MAGIC     COUNT(*) as total_rollbacks,
# MAGIC     SUM(r.TablesAffected) as tables_affected,
# MAGIC     SUM(r.TablesSucceeded) as tables_succeeded,
# MAGIC     SUM(r.TablesFailed) as tables_failed,
# MAGIC     AVG(r.QualityScore) as avg_quality_score,
# MAGIC     SUM(CASE WHEN r.ValidationPassed = true THEN 1 ELSE 0 END) as successful_rollbacks
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC GROUP BY DATE_FORMAT(r.RollbackDate, 'yyyy-MM')
# MAGIC ORDER BY rollback_month DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 6.2: Most frequently rolled back tables
# MAGIC SELECT
# MAGIC     a.TableName,
# MAGIC     COUNT(*) as restore_attempts,
# MAGIC     SUM(CASE WHEN a.Status = 'Success' THEN 1 ELSE 0 END) as successful,
# MAGIC     SUM(CASE WHEN a.Status = 'Error' THEN 1 ELSE 0 END) as failed
# MAGIC FROM metadata.SyncAuditLog a
# MAGIC WHERE a.Operation = 'TableRestore'
# MAGIC AND a.StartTime >= ADD_MONTHS(CURRENT_DATE(), -3)
# MAGIC GROUP BY a.TableName
# MAGIC ORDER BY restore_attempts DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 6.3: Rollback duration analysis
# MAGIC SELECT
# MAGIC     r.CheckpointName,
# MAGIC     r.RollbackDate,
# MAGIC     r.TablesAffected,
# MAGIC     MIN(a.StartTime) as rollback_start,
# MAGIC     MAX(a.EndTime) as rollback_end,
# MAGIC     CAST((UNIX_TIMESTAMP(MAX(a.EndTime)) - UNIX_TIMESTAMP(MIN(a.StartTime))) / 60 AS INT) as total_duration_minutes
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC INNER JOIN metadata.SyncAuditLog a ON r.PipelineRunId = a.PipelineRunId
# MAGIC WHERE a.Operation IN ('RollbackValidation', 'TableRestore', 'PostRollbackValidation')
# MAGIC AND r.RollbackDate >= ADD_MONTHS(CURRENT_DATE(), -1)
# MAGIC GROUP BY r.CheckpointName, r.RollbackDate, r.TablesAffected, r.PipelineRunId
# MAGIC ORDER BY total_duration_minutes DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 6.4: Data change summary from rollbacks
# MAGIC SELECT
# MAGIC     DATE_FORMAT(r.RollbackDate, 'yyyy-MM-dd') as rollback_date,
# MAGIC     r.CheckpointName,
# MAGIC     r.RecordsRestored,
# MAGIC     r.RecordsRemoved,
# MAGIC     (r.RecordsRestored - r.RecordsRemoved) as net_change,
# MAGIC     r.QualityScore
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC WHERE r.RollbackDate >= ADD_MONTHS(CURRENT_DATE(), -1)
# MAGIC ORDER BY r.RollbackDate DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 7: HEALTH CHECK QUERIES
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 7.1: Overall system health check
# MAGIC SELECT
# MAGIC     'Active Checkpoints' as metric,
# MAGIC     COUNT(*) as value
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE IsActive = true
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Rollbacks (Last 7 Days)',
# MAGIC     COUNT(*)
# MAGIC FROM metadata.RollbackComparisonReports
# MAGIC WHERE RollbackDate >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Failed Rollbacks (Last 30 Days)',
# MAGIC     COUNT(*)
# MAGIC FROM metadata.RollbackComparisonReports
# MAGIC WHERE ValidationPassed = false
# MAGIC AND RollbackDate >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Checkpoints Expiring Soon (7 Days)',
# MAGIC     COUNT(*)
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE IsActive = true
# MAGIC AND CAST(RetentionDate AS DATE) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), 7);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 7.2: Identify potential issues
# MAGIC SELECT
# MAGIC     'Issue' as issue_type,
# MAGIC     'Description' as description,
# MAGIC     0 as count
# MAGIC WHERE 1=0  -- Template structure
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Expired Checkpoints Not Deactivated',
# MAGIC     'Checkpoints past retention date still marked active',
# MAGIC     COUNT(*)
# MAGIC FROM metadata.CheckpointHistory
# MAGIC WHERE IsActive = true
# MAGIC AND RetentionDate < CURRENT_TIMESTAMP()
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Failed Restores',
# MAGIC     'Table restoration failures in last 7 days',
# MAGIC     COUNT(*)
# MAGIC FROM metadata.SyncAuditLog
# MAGIC WHERE Operation = 'TableRestore'
# MAGIC AND Status = 'Error'
# MAGIC AND StartTime >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Low Quality Rollbacks',
# MAGIC     'Rollbacks with quality score < 90% in last 30 days',
# MAGIC     COUNT(*)
# MAGIC FROM metadata.RollbackComparisonReports
# MAGIC WHERE QualityScore < 90
# MAGIC AND RollbackDate >= DATE_SUB(CURRENT_DATE(), 30);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- SECTION 8: CLEANUP & MAINTENANCE QUERIES
# MAGIC -- ============================================================
# MAGIC 
# MAGIC -- 8.1: Find old completed rollback snapshots (for potential cleanup)
# MAGIC -- These can be archived after 90 days
# MAGIC SELECT
# MAGIC     s.SnapshotId,
# MAGIC     s.PipelineRunId,
# MAGIC     s.TableName,
# MAGIC     s.SnapshotDate,
# MAGIC     DATEDIFF(CURRENT_DATE(), CAST(s.SnapshotDate AS DATE)) as days_old
# MAGIC FROM metadata.RollbackStateSnapshots s
# MAGIC WHERE s.SnapshotDate < DATE_SUB(CURRENT_DATE(), 90)
# MAGIC ORDER BY s.SnapshotDate;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 8.2: Archive old comparison reports (informational)
# MAGIC -- Shows reports older than 1 year that could be archived
# MAGIC SELECT
# MAGIC     r.ReportId,
# MAGIC     r.RollbackDate,
# MAGIC     r.CheckpointName,
# MAGIC     DATEDIFF(CURRENT_DATE(), CAST(r.RollbackDate AS DATE)) as days_old
# MAGIC FROM metadata.RollbackComparisonReports r
# MAGIC WHERE r.RollbackDate < ADD_MONTHS(CURRENT_DATE(), -12)
# MAGIC ORDER BY r.RollbackDate;

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
