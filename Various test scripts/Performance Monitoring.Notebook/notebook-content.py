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
# MAGIC SELECT 
# MAGIC     TableName,
# MAGIC     Operation,
# MAGIC     Status,
# MAGIC     DATEDIFF(minute, StartTime, EndTime) as DurationMinutes,
# MAGIC     RowsProcessed,
# MAGIC     RowsDeleted,
# MAGIC     StartTime,
# MAGIC     EndTime
# MAGIC FROM metadata.SyncAuditLog
# MAGIC WHERE PipelineRunId = (
# MAGIC     SELECT TOP 1 PipelineRunId 
# MAGIC     FROM metadata.SyncAuditLog 
# MAGIC     ORDER BY CreatedDate DESC
# MAGIC )
# MAGIC ORDER BY StartTime


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
