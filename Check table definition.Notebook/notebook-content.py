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

table_name = "default_table_name"
schema_name = "default_schema_name"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# Define tables that require manual creation
manual_creation_tables = ['activitypointer']

# Initialize results dictionary for pipeline output
results = {
    "table_exists": False,
    "schema_changed": False,
    "required_actions": [],  # Array to handle multiple simultaneous actions
    "current_columns": [],
    "error_message": "",
    "full_table_name": f"{schema_name}.{table_name}"
}

try:
    full_table_name = f"{schema_name}.{table_name}"
    # Check if table exists
    tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
    table_exists = any(row['tableName'].lower() == table_name.lower() for row in tables)
   
    results["table_exists"] = table_exists
   
    if not table_exists:
        # Check if this table requires manual creation
        if table_name in manual_creation_tables:
            # ERROR OUT: Table needs manual creation - stop pipeline execution
            error_msg = f"Table '{table_name}' does not exist and requires manual creation. Please create the table manually before running the pipeline."
            print(f"ERROR: {error_msg}")
            raise Exception(error_msg)
        else:
            # Table doesn't exist, need to create it AND add tracking columns
            results["required_actions"].append("CREATE_TABLE")
            results["required_actions"].append("ADD_TRACKING_COLUMNS")
    else:
        # Table exists, validate schema
        results["required_actions"].append("VALIDATE_SCHEMA")
        
        # Get current schema
        current_schema = spark.sql(f"DESCRIBE {full_table_name}").collect()
        results["current_columns"] = [row['col_name'] for row in current_schema if row['col_name'] not in ['', '# Partitioning']]
       
        # Check if tracking columns exist
        tracking_columns = ['IsDeleted', 'IsPurged', 'DeletedDate', 'PurgedDate', 'LastSynced']
        existing_columns = [col.lower() for col in results["current_columns"]]
        missing_tracking = [col for col in tracking_columns if col.lower() not in existing_columns]
       
        if missing_tracking:
            results["required_actions"].append("ADD_TRACKING_COLUMNS")
   
    print(f"Schema check results: {results}")
   
except Exception as e:
    # If it's our manual creation error, re-raise it to fail the activity
    if "requires manual creation" in str(e):
        raise e
    else:
        # For other errors, log and re-raise
        results["error_message"] = str(e)
        print(f"Error in schema check: {e}")
        raise Exception(f"Schema check failed for table {table_name}: {str(e)}")

# Only reach here if no errors occurred
# Output for pipeline consumption with proper JSON formatting
mssparkutils.notebook.exit(json.dumps(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
