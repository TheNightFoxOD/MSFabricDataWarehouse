-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4aee8a32-be91-489f-89f3-1a819b188807",
-- META       "default_lakehouse_name": "Master_Bronze",
-- META       "default_lakehouse_workspace_id": "b0f83c07-a701-49bb-a165-e06ca0ee4000",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "4aee8a32-be91-489f-89f3-1a819b188807"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Create new schemas
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS dataverse;
CREATE SCHEMA IF NOT EXISTS temp;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- 1. PipelineConfig: Master control table with purge tracking
-- MAGIC CREATE TABLE IF NOT EXISTS metadata.PipelineConfig (
-- MAGIC     TableId STRING NOT NULL,
-- MAGIC     TableName STRING NOT NULL,
-- MAGIC     SchemaName STRING NOT NULL,
-- MAGIC     PrimaryKeyColumn STRING NOT NULL,
-- MAGIC     SyncEnabled BOOLEAN NOT NULL,
-- MAGIC     TrackDeletes BOOLEAN NOT NULL,
-- MAGIC     LastPurgeDate TIMESTAMP,
-- MAGIC     PurgeRecordCount INT,
-- MAGIC     LastDailySync TIMESTAMP,
-- MAGIC     CreatedDate TIMESTAMP NOT NULL,
-- MAGIC     ModifiedDate TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO PipelineConfig 
(TableId, TableName, SchemaName, PrimaryKeyColumn, SyncEnabled, TrackDeletes, CreatedDate, ModifiedDate)
SELECT 'a1b2c3d4-e5f6-7890-1234-567890abcdef', 'account', 'dataverse', 'accountid', true, true, current_timestamp(), current_timestamp()
UNION ALL
SELECT 'b2c3d4e5-f6g7-8901-2345-6789012bcdef', 'donation', 'dataverse', 'donationid', true, true, current_timestamp(), current_timestamp()
UNION ALL
SELECT 'c3d4e5f6-g7h8-9012-3456-78901234cdef', 'activitypointer', 'dataverse', 'activityid', true, true, current_timestamp(), current_timestamp();

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # Databricks notebook source
-- MAGIC ## PipelineConfig Seeder from Dataverse JSON
-- MAGIC # Populate pipelineconfig table from Dataverse EntityDefinitions JSON
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC import json
-- MAGIC import uuid
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType
-- MAGIC from pyspark.sql.functions import when, col, count as spark_count
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC ## Step 1: Paste Your JSON Data Here
-- MAGIC # Copy what's needed from the JSON response of: https://yourorg.crm.dynamics.com/api/data/v9.2/EntityDefinitions?$select=LogicalName,PrimaryIdAttribute
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC # Paste your complete JSON response here
-- MAGIC json_data = """
-- MAGIC {
-- MAGIC     "@odata.context": "https://od-master.crm4.dynamics.com/api/data/v9.2/$metadata#EntityDefinitions(LogicalName,PrimaryIdAttribute)",
-- MAGIC     "value": [
-- MAGIC         {
-- MAGIC             "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84",
-- MAGIC             "LogicalName": "account",
-- MAGIC             "PrimaryIdAttribute": "accountid"
-- MAGIC         }
-- MAGIC     ]
-- MAGIC }
-- MAGIC """
-- MAGIC 
-- MAGIC # Parse the JSON data
-- MAGIC try:
-- MAGIC     parsed_data = json.loads(json_data)
-- MAGIC     entities = parsed_data.get('value', [])
-- MAGIC     print(f"✅ Successfully parsed JSON with {len(entities)} entities")
-- MAGIC     
-- MAGIC     # Show sample entities
-- MAGIC     print(f"Sample entities:")
-- MAGIC     for i, entity in enumerate(entities[:5]):
-- MAGIC         print(f"  {i+1}. {entity['LogicalName']} -> {entity['PrimaryIdAttribute']}")
-- MAGIC     if len(entities) > 5:
-- MAGIC         print(f"  ... and {len(entities) - 5} more entities")
-- MAGIC         
-- MAGIC except json.JSONDecodeError as e:
-- MAGIC     print(f"❌ Error parsing JSON: {e}")
-- MAGIC     entities = []
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC ## Step 2: Generate PipelineConfig Records
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC def generate_pipeline_config_records(entities):
-- MAGIC     """
-- MAGIC     Generate PipelineConfig records from Dataverse entities
-- MAGIC     Schema based on: [Master_Bronze].[metadata].[pipelineconfig]
-- MAGIC     """
-- MAGIC     records = []
-- MAGIC     current_time = datetime.now()
-- MAGIC     
-- MAGIC     for entity in entities:
-- MAGIC         logical_name = entity.get('LogicalName', '')
-- MAGIC         primary_key = entity.get('PrimaryIdAttribute', '')
-- MAGIC         
-- MAGIC         # Skip if essential fields are missing
-- MAGIC         if not logical_name or not primary_key:
-- MAGIC             print(f"⚠️  Skipping entity with missing data: {entity}")
-- MAGIC             continue
-- MAGIC         
-- MAGIC         # Generate TableId (GUID)
-- MAGIC         table_id = str(uuid.uuid4())
-- MAGIC         
-- MAGIC         # Create record matching the schema
-- MAGIC         record = {
-- MAGIC             'TableId': table_id,
-- MAGIC             'TableName': logical_name,
-- MAGIC             'SchemaName': 'dataverse',  # Standard for Dataverse
-- MAGIC             'PrimaryKeyColumn': primary_key,
-- MAGIC             'SyncEnabled': True,  # Enable all tables by default
-- MAGIC             'TrackDeletes': True,  # Enable delete tracking for all
-- MAGIC             'LastPurgeDate': None,  # Will be set after first purge
-- MAGIC             'PurgeRecordCount': None,  # Will be set after first purge
-- MAGIC             'LastDailySync': None,  # Will be set after first sync
-- MAGIC             'CreatedDate': current_time,
-- MAGIC             'ModifiedDate': current_time
-- MAGIC         }
-- MAGIC         
-- MAGIC         records.append(record)
-- MAGIC     
-- MAGIC     return records
-- MAGIC 
-- MAGIC # Generate the records
-- MAGIC if entities:
-- MAGIC     config_records = generate_pipeline_config_records(entities)
-- MAGIC     print(f"✅ Parsed {len(config_records)} PipelineConfig records")
-- MAGIC     
-- MAGIC     # Show sample records
-- MAGIC     print(f"\nSample parsed records:")
-- MAGIC     for i, record in enumerate(config_records[:3]):
-- MAGIC         print(f"  {i+1}. {record['TableName']} -> {record['PrimaryKeyColumn']} (ID: {record['TableId'][:8]}...)")
-- MAGIC else:
-- MAGIC     config_records = []
-- MAGIC     print("❌ No entities to process")
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC ## Step 3: Create DataFrame with Correct Schema
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC # Define schema to match the actual pipelineconfig table structure
-- MAGIC pipeline_config_schema = StructType([
-- MAGIC     StructField("TableId", StringType(), False),
-- MAGIC     StructField("TableName", StringType(), False),
-- MAGIC     StructField("SchemaName", StringType(), False),
-- MAGIC     StructField("PrimaryKeyColumn", StringType(), False),
-- MAGIC     StructField("SyncEnabled", BooleanType(), False),
-- MAGIC     StructField("TrackDeletes", BooleanType(), False),
-- MAGIC     StructField("LastPurgeDate", TimestampType(), True),
-- MAGIC     StructField("PurgeRecordCount", IntegerType(), True),
-- MAGIC     StructField("LastDailySync", TimestampType(), True),
-- MAGIC     StructField("CreatedDate", TimestampType(), False),
-- MAGIC     StructField("ModifiedDate", TimestampType(), False)
-- MAGIC ])
-- MAGIC 
-- MAGIC # Create DataFrame
-- MAGIC if config_records:
-- MAGIC     df = spark.createDataFrame(config_records, schema=pipeline_config_schema)
-- MAGIC     
-- MAGIC     print("✅ DataFrame created successfully")
-- MAGIC     print(f"Schema validation:")
-- MAGIC     df.printSchema()
-- MAGIC     
-- MAGIC     print(f"\nSample data preview:")
-- MAGIC     df.select("TableName", "PrimaryKeyColumn", "SyncEnabled", "TrackDeletes", "CreatedDate").show(10, truncate=False)
-- MAGIC     
-- MAGIC else:
-- MAGIC     df = None
-- MAGIC     print("❌ No data to create DataFrame")
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC ## Step 4: Check for Existing Records and Prevent Duplicates
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC def check_existing_tables():
-- MAGIC     """Check which tables already exist in pipelineconfig to prevent duplicates"""
-- MAGIC     try:
-- MAGIC         existing_df = spark.sql("SELECT TableName FROM Master_Bronze.metadata.pipelineconfig")
-- MAGIC         existing_tables = [row.TableName for row in existing_df.collect()]
-- MAGIC         return set(existing_tables)
-- MAGIC     except Exception as e:
-- MAGIC         print(f"ℹ️  Could not read existing records (table might be empty): {e}")
-- MAGIC         return set()
-- MAGIC 
-- MAGIC def filter_new_records(config_records, existing_tables):
-- MAGIC     """Filter out records that already exist"""
-- MAGIC     new_records = []
-- MAGIC     duplicate_count = 0
-- MAGIC     
-- MAGIC     for record in config_records:
-- MAGIC         if record['TableName'] in existing_tables:
-- MAGIC             duplicate_count += 1
-- MAGIC             print(f"⚠️  Skipping duplicate: {record['TableName']}")
-- MAGIC         else:
-- MAGIC             new_records.append(record)
-- MAGIC     
-- MAGIC     print(f"📊 Summary:")
-- MAGIC     print(f"   - Total entities from JSON: {len(config_records)}")
-- MAGIC     print(f"   - Already exist: {duplicate_count}")
-- MAGIC     print(f"   - New to insert: {len(new_records)}")
-- MAGIC     
-- MAGIC     return new_records
-- MAGIC 
-- MAGIC # Check for existing records
-- MAGIC if config_records:
-- MAGIC     print("🔍 Checking for existing records...")
-- MAGIC     existing_tables = check_existing_tables()
-- MAGIC     print(f"Found {len(existing_tables)} existing tables in pipelineconfig")
-- MAGIC     
-- MAGIC     # Filter out duplicates
-- MAGIC     new_records = filter_new_records(config_records, existing_tables)
-- MAGIC     
-- MAGIC     if new_records:
-- MAGIC         # Create DataFrame with only new records
-- MAGIC         df_new = spark.createDataFrame(new_records, schema=pipeline_config_schema)
-- MAGIC         print(f"✅ Prepared {len(new_records)} new records for insertion")
-- MAGIC     else:
-- MAGIC         df_new = None
-- MAGIC         print("ℹ️  No new records to insert - all tables already exist")
-- MAGIC else:
-- MAGIC     new_records = []
-- MAGIC     df_new = None
-- MAGIC     print("❌ No records generated from JSON")
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC ## Step 5: Insert New Records into PipelineConfig Table
-- MAGIC 
-- MAGIC # COMMAND ----------
-- MAGIC 
-- MAGIC if df_new is not None and len(new_records) > 0:
-- MAGIC     try:
-- MAGIC         # Write to the pipelineconfig table
-- MAGIC         df_new.write \
-- MAGIC           .format("delta") \
-- MAGIC           .mode("append") \
-- MAGIC           .option("mergeSchema", "true") \
-- MAGIC           .saveAsTable("Master_Bronze.metadata.pipelineconfig")
-- MAGIC         
-- MAGIC         print(f"✅ Successfully inserted {len(new_records)} new records into pipelineconfig table")
-- MAGIC         
-- MAGIC     except Exception as e:
-- MAGIC         print(f"❌ Error inserting records: {e}")
-- MAGIC         print("Trying alternative table reference...")
-- MAGIC         
-- MAGIC         try:
-- MAGIC             # Alternative approach using direct table name
-- MAGIC             df_new.write \
-- MAGIC               .format("delta") \
-- MAGIC               .mode("append") \
-- MAGIC               .option("mergeSchema", "true") \
-- MAGIC               .saveAsTable("pipelineconfig")
-- MAGIC             
-- MAGIC             print(f"✅ Successfully inserted {len(new_records)} records using alternative method")
-- MAGIC             
-- MAGIC         except Exception as e2:
-- MAGIC             print(f"❌ Alternative method also failed: {e2}")
-- MAGIC             print("Please check your table reference and permissions")
-- MAGIC 
-- MAGIC else:
-- MAGIC     print("ℹ️  No new records to insert")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from metadata.PipelineConfig

-- update metadata.PipelineConfig
-- set SchemaName = 'dataverse'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

truncate table metadata.PipelineConfig

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark",
-- META   "frozen": true,
-- META   "editable": false
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df = spark.sql("SELECT * FROM Master_Bronze.metadata.pipelineconfig LIMIT 1000")
-- MAGIC display(df)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- 2. SyncAuditLog: Execution tracking
-- MAGIC CREATE TABLE IF NOT EXISTS metadata.SyncAuditLog (
-- MAGIC     LogId STRING NOT NULL,
-- MAGIC     PipelineRunId STRING NOT NULL,
-- MAGIC     PipelineName STRING NOT NULL,
-- MAGIC     TableName STRING,
-- MAGIC     Operation STRING NOT NULL,
-- MAGIC     StartTime TIMESTAMP NOT NULL,
-- MAGIC     EndTime TIMESTAMP,
-- MAGIC     RowsProcessed INT,
-- MAGIC     RowsDeleted INT,
-- MAGIC     RowsPurged INT,
-- MAGIC     Status STRING NOT NULL,
-- MAGIC     ErrorMessage STRING,
-- MAGIC     RetryCount INT NOT NULL,
-- MAGIC     CreatedDate TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- 3. CheckpointHistory: Fallback management
-- MAGIC CREATE TABLE IF NOT EXISTS metadata.CheckpointHistory (
-- MAGIC     CheckpointId STRING NOT NULL,
-- MAGIC     CheckpointName STRING NOT NULL,
-- MAGIC     CheckpointType STRING NOT NULL,
-- MAGIC     CreatedDate TIMESTAMP NOT NULL,
-- MAGIC     TablesIncluded INT NOT NULL,
-- MAGIC     TotalRows BIGINT,
-- MAGIC     ValidationStatus STRING NOT NULL,
-- MAGIC     RetentionDate DATE NOT NULL,
-- MAGIC     IsActive BOOLEAN NOT NULL
-- MAGIC ) USING DELTA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE IF NOT EXISTS metadata.DataValidation (
-- MAGIC     ValidationId STRING NOT NULL,
-- MAGIC     ValidationDate TIMESTAMP NOT NULL,
-- MAGIC     TableName STRING NOT NULL,
-- MAGIC     SourceRowCount INT,
-- MAGIC     BronzeRowCount INT NOT NULL,
-- MAGIC     ActiveRowCount INT NOT NULL,
-- MAGIC     DeletedRowCount INT NOT NULL,
-- MAGIC     PurgedRowCount INT NOT NULL,
-- MAGIC     ValidationPassed BOOLEAN NOT NULL
-- MAGIC ) USING DELTA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

OPTIMIZE [metadata].PipelineConfig;
OPTIMIZE [metadata].SyncAuditLog;
OPTIMIZE [metadata].CheckpointHistory;
OPTIMIZE [metadata].DataValidation;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
