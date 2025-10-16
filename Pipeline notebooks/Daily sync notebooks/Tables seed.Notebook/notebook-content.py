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
# META         },
# META         {
# META           "id": "234e6789-2254-455f-b2b2-36d881cb1c17"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

STAGING_LAKEHOUSE = "STAGING_LAKEHOUSE_default"
BRONZE_LAKEHOUSE = "BRONZE_LAKEHOUSE_default"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC # Databricks notebook source
# MAGIC ## Enhanced PipelineConfig Seeder from Link to Fabric Tables
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC import uuid
# MAGIC from datetime import datetime
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC # STAGING_LAKEHOUSE = "Dataverse_Staging"
# MAGIC # ENTITY_DEFINITIONS_TABLE = "Bronze.dbo.entity_definitions"
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Load Entity Definitions from Dataflow Table
# MAGIC 
# MAGIC ENTITY_DEFINITIONS_TABLE = f"{BRONZE_LAKEHOUSE}.dbo.entity_definitions"
# MAGIC 
# MAGIC def load_entity_definitions_from_table(table_name):
# MAGIC     try:
# MAGIC         entities_df = spark.table(table_name)
# MAGIC         entities_df = entities_df.select("LogicalName", "PrimaryIdAttribute")
# MAGIC         entities = [
# MAGIC             {
# MAGIC                 'LogicalName': row.LogicalName,
# MAGIC                 'PrimaryIdAttribute': row.PrimaryIdAttribute
# MAGIC             }
# MAGIC             for row in entities_df.collect()
# MAGIC             if row.LogicalName and row.PrimaryIdAttribute
# MAGIC         ]
# MAGIC         return entities
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Error loading from table {table_name}: {e}")
# MAGIC         print("Make sure the Dataflow has been run at least once")
# MAGIC         return []
# MAGIC 
# MAGIC print(f"📊 Loading entity definitions from: {ENTITY_DEFINITIONS_TABLE}")
# MAGIC entities = load_entity_definitions_from_table(ENTITY_DEFINITIONS_TABLE)
# MAGIC 
# MAGIC if entities:
# MAGIC     print(f"✅ Loaded {len(entities)} entity definitions")
# MAGIC     entity_lookup = {
# MAGIC         entity['LogicalName']: entity['PrimaryIdAttribute'] 
# MAGIC         for entity in entities
# MAGIC     }
# MAGIC     print(f"📋 Created lookup dictionary with {len(entity_lookup)} entities")
# MAGIC else:
# MAGIC     print("❌ Failed to load entity definitions")
# MAGIC     entity_lookup = {}
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Get Staging Tables from Link to Fabric Lakehouse
# MAGIC 
# MAGIC def get_staging_tables(lakehouse_name):
# MAGIC     try:
# MAGIC         tables_df = spark.sql(f"SHOW TABLES IN {lakehouse_name}.dbo")
# MAGIC         table_names = [row.tableName for row in tables_df.collect()]
# MAGIC         print(f"✅ Found {len(table_names)} tables in {lakehouse_name}.dbo")
# MAGIC         return set(table_names)
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Error querying staging lakehouse: {e}")
# MAGIC         return set()
# MAGIC 
# MAGIC staging_tables = get_staging_tables(STAGING_LAKEHOUSE)
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Match Tables and Generate PipelineConfig Records
# MAGIC 
# MAGIC def generate_pipeline_config_records(staging_tables, entity_lookup):
# MAGIC     records = []
# MAGIC     current_time = datetime.now()
# MAGIC     matched_count = 0
# MAGIC     missing_in_definitions = 0
# MAGIC     
# MAGIC     for table_name in staging_tables:
# MAGIC         if table_name in entity_lookup:
# MAGIC             primary_key = entity_lookup[table_name]
# MAGIC             table_id = str(uuid.uuid4())
# MAGIC             
# MAGIC             record = {
# MAGIC                 'TableId': table_id,
# MAGIC                 'TableName': table_name,
# MAGIC                 'SchemaName': 'dataverse',
# MAGIC                 'PrimaryKeyColumn': primary_key,
# MAGIC                 'SyncEnabled': True,
# MAGIC                 'TrackDeletes': True,
# MAGIC                 'LastPurgeDate': None,
# MAGIC                 'PurgeRecordCount': None,
# MAGIC                 'LastDailySync': None,
# MAGIC                 'CreatedDate': current_time,
# MAGIC                 'ModifiedDate': current_time
# MAGIC             }
# MAGIC             records.append(record)
# MAGIC             matched_count += 1
# MAGIC         else:
# MAGIC             missing_in_definitions += 1
# MAGIC             print(f"⚠️  Table '{table_name}' in staging but not in entity definitions - skipping")
# MAGIC     
# MAGIC     print(f"\n📊 Matching Summary:")
# MAGIC     print(f"   - Tables in staging: {len(staging_tables)}")
# MAGIC     print(f"   - Entities in definitions: {len(entity_lookup)}")
# MAGIC     print(f"   - ✅ Matched (will seed): {matched_count}")
# MAGIC     print(f"   - ⚠️  In staging but not in definitions: {missing_in_definitions}")
# MAGIC     
# MAGIC     return records
# MAGIC 
# MAGIC if staging_tables and entity_lookup:
# MAGIC     config_records = generate_pipeline_config_records(staging_tables, entity_lookup)
# MAGIC     print(f"\n✅ Generated {len(config_records)} records")
# MAGIC else:
# MAGIC     print("❌ Cannot generate records")
# MAGIC     config_records = []
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Create DataFrame
# MAGIC 
# MAGIC pipeline_config_schema = StructType([
# MAGIC     StructField("TableId", StringType(), False),
# MAGIC     StructField("TableName", StringType(), False),
# MAGIC     StructField("SchemaName", StringType(), False),
# MAGIC     StructField("PrimaryKeyColumn", StringType(), False),
# MAGIC     StructField("SyncEnabled", BooleanType(), False),
# MAGIC     StructField("TrackDeletes", BooleanType(), False),
# MAGIC     StructField("LastPurgeDate", TimestampType(), True),
# MAGIC     StructField("PurgeRecordCount", IntegerType(), True),
# MAGIC     StructField("LastDailySync", TimestampType(), True),
# MAGIC     StructField("CreatedDate", TimestampType(), False),
# MAGIC     StructField("ModifiedDate", TimestampType(), False)
# MAGIC ])
# MAGIC 
# MAGIC if config_records:
# MAGIC     df = spark.createDataFrame(config_records, schema=pipeline_config_schema)
# MAGIC     print("✅ DataFrame created")
# MAGIC else:
# MAGIC     df = None
# MAGIC     print("❌ No data to create DataFrame")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Check for Existing Records
# MAGIC 
# MAGIC def check_existing_tables():
# MAGIC     try:
# MAGIC         existing_df = spark.sql(f"SELECT TableName FROM {BRONZE_LAKEHOUSE}.metadata.pipelineconfig")
# MAGIC         existing_tables = [row.TableName for row in existing_df.collect()]
# MAGIC         return set(existing_tables)
# MAGIC     except Exception as e:
# MAGIC         print(f"ℹ️  Table might be empty: {e}")
# MAGIC         return set()
# MAGIC 
# MAGIC def filter_new_records(config_records, existing_tables):
# MAGIC     new_records = []
# MAGIC     duplicate_count = 0
# MAGIC     
# MAGIC     for record in config_records:
# MAGIC         if record['TableName'] in existing_tables:
# MAGIC             duplicate_count += 1
# MAGIC         else:
# MAGIC             new_records.append(record)
# MAGIC     
# MAGIC     print(f"\n📊 Duplicate Check:")
# MAGIC     print(f"   - Total from matching: {len(config_records)}")
# MAGIC     print(f"   - Already exist: {duplicate_count}")
# MAGIC     print(f"   - 🆕 New to insert: {len(new_records)}")
# MAGIC     
# MAGIC     return new_records
# MAGIC 
# MAGIC if config_records:
# MAGIC     print("🔍 Checking for existing records...")
# MAGIC     existing_tables = check_existing_tables()
# MAGIC     print(f"Found {len(existing_tables)} existing tables")
# MAGIC     
# MAGIC     new_records = filter_new_records(config_records, existing_tables)
# MAGIC     
# MAGIC     if new_records:
# MAGIC         df_new = spark.createDataFrame(new_records, schema=pipeline_config_schema)
# MAGIC         print(f"✅ Prepared {len(new_records)} new records")
# MAGIC     else:
# MAGIC         df_new = None
# MAGIC         print("ℹ️  No new records to insert")
# MAGIC else:
# MAGIC     new_records = []
# MAGIC     df_new = None
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Insert New Records
# MAGIC 
# MAGIC if df_new is not None and len(new_records) > 0:
# MAGIC     try:
# MAGIC         df_new.write \
# MAGIC           .format("delta") \
# MAGIC           .mode("append") \
# MAGIC           .option("mergeSchema", "true") \
# MAGIC           .saveAsTable(f"{BRONZE_LAKEHOUSE}.metadata.pipelineconfig")
# MAGIC         
# MAGIC         print(f"✅ Successfully inserted {len(new_records)} records")
# MAGIC         print(f"\n🎉 Seeding Complete!")
# MAGIC         print(f"   - Staging tables scanned: {len(staging_tables)}")
# MAGIC         print(f"   - Tables matched: {len(config_records)}")
# MAGIC         print(f"   - New tables seeded: {len(new_records)}")
# MAGIC         
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Error inserting records: {e}")
# MAGIC else:
# MAGIC     print("ℹ️  No new records to insert")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC ## Verify Results
# MAGIC 
# MAGIC print("\n🔍 Current PipelineConfig State:")
# MAGIC current_config_query = """
# MAGIC     SELECT TableName, SchemaName, PrimaryKeyColumn, SyncEnabled, CreatedDate
# MAGIC     FROM {BRONZE_LAKEHOUSE}.metadata.pipelineconfig
# MAGIC     ORDER BY TableName
# MAGIC """.format(
# MAGIC     BRONZE_LAKEHOUSE=BRONZE_LAKEHOUSE,
# MAGIC )
# MAGIC current_config = spark.sql(current_config_query)
# MAGIC 
# MAGIC print(f"\nTotal tables in PipelineConfig: {current_config.count()}")
# MAGIC current_config.show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
