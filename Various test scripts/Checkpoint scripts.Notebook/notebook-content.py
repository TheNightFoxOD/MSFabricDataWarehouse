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

# CELL ********************

# MAGIC %%sql
# MAGIC select * from metadata.checkpointhistory
# MAGIC where 1=1
# MAGIC -- and RetentionDate < CURRENT_DATE()
# MAGIC and IsActive = true
# MAGIC ORDER by CreatedDate desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC update metadata.CheckpointHistory 
# MAGIC set IsActive = true

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
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
