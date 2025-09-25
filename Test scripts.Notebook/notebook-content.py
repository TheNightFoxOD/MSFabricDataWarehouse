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
# MAGIC -- describe table dataverse.account
# MAGIC select od_groupname, accountid, address1_city, od_startdate, IsDeleted, LastSynced from dataverse.account
# MAGIC where 1=1
# MAGIC -- and IsDeleted = true
# MAGIC order by modifiedon desc 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from metadata.syncauditlog
# MAGIC order by CreatedDate desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from metadata.checkpointhistory
# MAGIC order by CreatedDate desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- select * from metadata.pipelineconfig
# MAGIC update metadata.pipelineconfig
# MAGIC set SyncEnable = false

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE dataverse.account
# MAGIC     DROP COLUMN LastSynced, IsDeleted, IsPurged, DeletedDate, PurgedDate
# MAGIC -- ALTER TABLE dataverse.account SET TBLPROPERTIES (
# MAGIC --    'delta.columnMapping.mode' = 'name',
# MAGIC --    'delta.minReaderVersion' = '2',
# MAGIC --    'delta.minWriterVersion' = '5')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
