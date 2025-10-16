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

# PARAMETERS CELL ********************

schema_name = "default_schema_name"
table_name = "default_table_name"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

try:
    full_table_name = f"{schema_name}.{table_name}"
    
    # Check if table is empty
    row_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
    
    is_empty = (row_count == 0)
    
    print(f"Table {table_name} emptiness check:")
    print(f"  - Row count: {row_count}")
    print(f"  - Is empty: {is_empty}")
    
    if not is_empty:
        # Table is not empty - fail the notebook to prevent append
        error_message = f"Table {table_name} is not empty (contains {row_count} records). Cannot proceed with initial append to prevent duplicate data."
        print(f"ERROR: {error_message}")
        raise Exception(error_message)
    
    # Table is empty - succeed and allow append to proceed
    result = {
        "status": "success",
        "table_name": table_name,
        "row_count": row_count,
        "is_empty": True,
        "action": "table_confirmed_empty_ready_for_append"
    }
    
    print(f"SUCCESS: Table {table_name} is empty and ready for initial data append")
    
except Exception as e:
    result = {
        "status": "error", 
        "error_message": str(e),
        "table_name": table_name
    }
    print(f"FAILURE: {e}")
    # Let the exception bubble up to fail the notebook
    raise

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
