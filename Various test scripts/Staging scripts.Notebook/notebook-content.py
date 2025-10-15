# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "234e6789-2254-455f-b2b2-36d881cb1c17",
# META       "default_lakehouse_name": "Dataverse_Master_Staging",
# META       "default_lakehouse_workspace_id": "b0f83c07-a701-49bb-a165-e06ca0ee4000",
# META       "known_lakehouses": [
# META         {
# META           "id": "234e6789-2254-455f-b2b2-36d881cb1c17"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- describe table dataverse.account
# MAGIC select od_donationid, SinkCreatedOn, SinkModifiedOn, IsDelete
# MAGIC  from od_donation
# MAGIC where 1=1
# MAGIC -- and IsDeleted = true
# MAGIC and (od_donationid = 'ca5c36ca-3511-ea11-a811-000d3a4aa1c2' -- purged
# MAGIC     or od_donationid = 'db7810ed-ab90-4d51-ad2f-52f2e2da3dd0' -- deleted
# MAGIC     or od_donationid = '0f6219ea-1ee8-4329-a8c4-63ba5ae335d7' -- deleted
# MAGIC     or od_donationid = '308a44af-6381-4f76-84fe-da38872cc7e5') -- new
# MAGIC -- order by modifiedon desc 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TBLPROPERTIES account ('primaryKey');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC describe account

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select od_groupname, od_temp1, od_temp2 from account
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
# MAGIC SHOW TABLES LIKE 'Temp2';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
