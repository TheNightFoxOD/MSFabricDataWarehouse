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
# MAGIC select od_groupname, accountid, address1_line1, IsDeleted, LastSynced, createdon, IsDeleted, DeletedDate, IsPurged, PurgedDate, createdon, modifiedon
# MAGIC  from dataverse.account
# MAGIC where 1=1
# MAGIC -- and IsDeleted = true
# MAGIC and (accountid = '0a204a8b-03f0-ee11-904b-000d3a498565' -- address updated
# MAGIC     or accountid = 'd6c03ef4-de74-ea11-a811-000d3a4aadc8' -- purged
# MAGIC     or accountid = '5cbf316c-0ac6-ee11-9079-000d3ab6fbae' -- deleted
# MAGIC     or accountid = '33f235ee-4fa0-f011-bbd3-6045bd9b7cb3') -- new
# MAGIC -- order by modifiedon desc 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT od_groupname, accountid, address1_line1, IsDeleted, LastSynced, createdon, IsDeleted, DeletedDate, IsPurged, PurgedDate, createdon, modifiedon
# MAGIC from dataverse.account
# MAGIC where 1=1
# MAGIC and IsPurged = true
# MAGIC 
# MAGIC -- update dataverse.account
# MAGIC -- set IsDeleted = NULL
# MAGIC -- where IsPurged = true

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
# MAGIC select * from metadata.pipelineconfig
# MAGIC where TableName = 'account'
# MAGIC -- update metadata.pipelineconfig
# MAGIC -- set LastPurgeDate = '2023-01-01'
# MAGIC -- where TableName = 'account'    

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
