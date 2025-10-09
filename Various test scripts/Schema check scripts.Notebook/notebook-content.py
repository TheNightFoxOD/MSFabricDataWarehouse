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
# META         },
# META         {
# META           "id": "234e6789-2254-455f-b2b2-36d881cb1c17"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC describe TABLE Dataverse_Master_Staging.account


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC describe TABLE  Master_Bronze.dataverse.account

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM metadata.syncauditlog 
# MAGIC WHERE 1=1
# MAGIC -- AND PipelineRunId = 'e13e5a80-cba7-4305-8002-c923f1ca61ef' 
# MAGIC   -- AND TableName = 'dataverse.account'
# MAGIC     -- and Operation = 'ColumnDrop'
# MAGIC ORDER BY CreatedDate DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from metadata.entity_definitions

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select od_groupname, od_temp1, od_temp2 from dataverse.account
# MAGIC where 1=1
# MAGIC -- and od_groupname = 'Test Group'
# MAGIC and (od_temp1 <> '' or od_temp2 <> '')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE metadata.pipelineconfig
# MAGIC SET SyncEnabled = true
# MAGIC where 1=1
# MAGIC and (TableName = 'account' or TableName = 'od_donation')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from metadata.pipelineconfig
# MAGIC where SyncEnabled = true

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES IN dataverse LIKE 'od_donation';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table Master_Bronze.dataverse.od_donation


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
