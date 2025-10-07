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

# PARAMETERS CELL ********************

table_name = "default_table"
schema_name = "default_schema"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# assign like this for manually created tables
# table_name = "activitypointer"
# schema_name = "dataverse"

try:
    full_table_name = f"{schema_name}.{table_name}"
    
    # Query PipelineConfig to get primary key information
    primary_key_query = f"""
    SELECT 
        PrimaryKeyColumn
    FROM metadata.PipelineConfig 
    WHERE TableName = '{table_name}' 
    AND SyncEnabled = true
    """
    
    print(f"Querying PipelineConfig for table: {table_name}")
    config_result = spark.sql(primary_key_query).collect()
    
    if not config_result:
        raise Exception(f"No configuration found for table {table_name} in PipelineConfig")
    
    primary_key_column = config_result[0]['PrimaryKeyColumn']
    print(f"Found primary key column: {primary_key_column}")
    
    # 1. Verify the primary key column exists in the table FIRST
    table_columns = spark.sql(f"DESCRIBE {full_table_name}").collect()
    available_columns = [row['col_name'].lower() for row in table_columns if row['col_name'] not in ['', '# Partitioning']]
    
    if primary_key_column.lower() not in available_columns:
        raise Exception(f"Primary key column '{primary_key_column}' not found in table {full_table_name}")
    
    # 2. Set table properties to document the primary key (governance)
    alter_properties_sql = f"""
    ALTER TABLE {full_table_name} 
    SET TBLPROPERTIES (
        'primaryKey' = '{primary_key_column}',
        'primaryKeySet' = 'true',
        'primaryKeySetDate' = '{spark.sql("SELECT current_timestamp()").collect()[0][0]}'
    )
    """
    
    print(f"Setting table properties for primary key")
    spark.sql(alter_properties_sql)
    
    # 3. Set auto-optimize properties (affects all future writes)
    auto_optimize_sql = f"""
    ALTER TABLE {full_table_name} 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    print(f"Setting auto-optimize properties for future writes")
    spark.sql(auto_optimize_sql)
    
    # 4. Confirm settings were applied
    table_properties = spark.sql(f"SHOW TBLPROPERTIES {full_table_name}").collect()
    properties_dict = {row['key']: row['value'] for row in table_properties}
    
    result = {
        "status": "success",
        "table_name": table_name,
        "primary_key_column": primary_key_column,
        "properties_set": "primaryKey" in properties_dict,
        "auto_optimize_enabled": "delta.autoOptimize.optimizeWrite" in properties_dict,
        "primary_key_value": properties_dict.get("primaryKey", ""),
        "action": "table_configured_for_data_loading"
    }
    
    print(f"Table configuration completed: {result}")
    
except Exception as e:
    result = {
        "status": "error", 
        "error_message": str(e),
        "table_name": table_name,
        "action": "table_configuration_failed"
    }
    print(f"Error configuring table: {e}")

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
