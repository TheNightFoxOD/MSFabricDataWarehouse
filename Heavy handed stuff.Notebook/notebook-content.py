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
# MAGIC truncate table Master_Bronze.dataverse.activitypointer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table Master_Bronze.dataverse.account

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table Master_Bronze.dataverse.od_donation

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table Master_Bronze.metadata.checkpointhistory

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table Master_Bronze.metadata.datavalidation

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table Master_Bronze.metadata.pipelineconfig

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table Master_Bronze.metadata.syncauditlog

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- select * from Master_Bronze.metadata.syncauditlog
# MAGIC -- order by CreatedDate DESC
# MAGIC 
# MAGIC 
# MAGIC SELECT Operation, Status, RowsProcessed, RowsDeleted, RowsPurged, StartTime, EndTime
# MAGIC FROM metadata.syncauditlog 
# MAGIC WHERE PipelineRunId = 'e13e5a80-cba7-4305-8002-c923f1ca61ef' 
# MAGIC   AND TableName = 'dataverse.account'
# MAGIC ORDER BY CreatedDate

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table dataverse.account

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table dataverse.od_donation

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table dataverse.contact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- describe table dataverse.activitypointer
# MAGIC select * from dataverse.activitypointer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from metadata.pipelineconfig
# MAGIC where TableName = 'account'
# MAGIC 
# MAGIC -- DELETE from metadata.pipelineconfig
# MAGIC -- where TableName = 'activitypointer'
# MAGIC --     or TableName = 'account'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from dbo.od_transaction
# MAGIC -- describe table dbo.od_transaction

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table dataverse.contact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dataverse.od_donation as target
# MAGIC USING temp.od_donation_staging as staging
# MAGIC ON CAST(target.od_donationid AS STRING) = CAST(staging.od_donationid AS STRING)
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     target.od_amount = staging.od_amount
# MAGIC WHEN NOT MATCHED THEN INSERT 
# MAGIC     (target.od_amount)
# MAGIC     VALUES (staging.od_amount)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dataverse.od_donation as target
# MAGIC USING temp.od_donation_staging as staging
# MAGIC ON CAST(target.od_donationid AS STRING) = CAST(staging.od_donationid AS STRING)
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     *
# MAGIC WHEN NOT MATCHED THEN INSERT 
# MAGIC     *

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- describe table temp.account_staging
# MAGIC -- select count(*) from temp.od_donation_staging
# MAGIC -- select * from temp.account_staging
# MAGIC -- where od_donationid = '0f6219ea-1ee8-4329-a8c4-63ba5ae335d7'
# MAGIC -- or od_donationid = '3b5f38d9-1c5c-48b1-af7d-2d63a4e2fe4f'
# MAGIC drop table temp.account_staging

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- select od_amount, od_mediacode
# MAGIC -- from dataverse.od_donation
# MAGIC -- where od_donationid = '0f6219ea-1ee8-4329-a8c4-63ba5ae335d7'
# MAGIC 
# MAGIC DESCRIBE dataverse.od_donation

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*)
# MAGIC from dataverse.od_donation

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select od_donationid, od_amount, LastSynced, IsDeleted, IsPurged
# MAGIC from dataverse.od_donation
# MAGIC where od_donationid = '0f6219ea-1ee8-4329-a8c4-63ba5ae335d7' -- updated
# MAGIC or od_donationid = '5048c7f2-eb30-4af4-bd1d-7ee19ff36dad' -- deleted
# MAGIC or od_donationid = '43cd37ec-1216-4ed5-bbc7-3022c75db947' -- deleted
# MAGIC or od_donationid = '3077df32-16c5-4b39-9f5e-14650df74626' -- added
# MAGIC order by LastSynced desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- select od_amount, od_mediacode, od_source
# MAGIC -- from temp.od_donation_staging
# MAGIC -- where od_donationid = '0f6219ea-1ee8-4329-a8c4-63ba5ae335d7'
# MAGIC 
# MAGIC -- DESCRIBE temp.od_donation_staging
# MAGIC 
# MAGIC DESCRIBE temp.od_donation_CurrentIDs
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE metadata.pipelineconfig
# MAGIC SET SyncEnabled = false
# MAGIC where TableName <> 'account'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Count tables in a specific schema
# MAGIC SELECT COUNT(*) AS TableCount
# MAGIC FROM INFORMATION_SCHEMA.TABLES
# MAGIC WHERE TABLE_TYPE = 'BASE TABLE'
# MAGIC   AND TABLE_SCHEMA = 'dataverse';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TBLPROPERTIES dataverse.od_donation ('primaryKey');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dataverse.account
# MAGIC where accountid is null

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
