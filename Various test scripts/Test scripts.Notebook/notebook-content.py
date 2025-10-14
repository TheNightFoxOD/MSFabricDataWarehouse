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
# MAGIC     or accountid = 'e4ac267e-2a40-ef11-8409-000d3a4c66d5') -- deleted
# MAGIC -- order by modifiedon desc 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- describe table dataverse.account
# MAGIC select od_donationid, IsDeleted, IsPurged, SinkCreatedOn, SinkModifiedOn, LastSynced
# MAGIC  from dataverse.od_donation
# MAGIC where 1=1
# MAGIC -- and IsDeleted = true
# MAGIC and (od_donationid = 'ca5c36ca-3511-ea11-a811-000d3a4aa1c2' -- purged
# MAGIC     or od_donationid = 'db7810ed-ab90-4d51-ad2f-52f2e2da3dd0' -- deleted
# MAGIC     or od_donationid = '308a44af-6381-4f76-84fe-da38872cc7e5') -- new
# MAGIC -- order by modifiedon desc 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- SELECT od_groupname, accountid, address1_line1, IsDeleted, LastSynced, createdon, IsDeleted, DeletedDate, IsPurged, PurgedDate, createdon, modifiedon
# MAGIC -- from dataverse.account
# MAGIC -- where 1=1
# MAGIC -- and IsPurged = true
# MAGIC 
# MAGIC -- update dataverse.account
# MAGIC -- set IsPurged = null, PurgedDate = null
# MAGIC -- where accountid = 'd6c03ef4-de74-ea11-a811-000d3a4aadc8'
# MAGIC 
# MAGIC update dataverse.account
# MAGIC set address1_line1 = 'None'
# MAGIC where accountid = '0a204a8b-03f0-ee11-904b-000d3a498565'
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC update dataverse.account
# MAGIC set IsDeleted = null, DeletedDate = null
# MAGIC where accountid = 'e4ac267e-2a40-ef11-8409-000d3a4c66d5'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from metadata.syncauditlog
# MAGIC where 1=1
# MAGIC     and Operation = 'CreateTable'
# MAGIC     and TableName = 'dataverse.account'
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC describe table dataverse.od_donation

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
