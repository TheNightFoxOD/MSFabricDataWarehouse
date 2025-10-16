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

table_name = "default_table"
schema_name = "default_schema"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime

# assignment for testing purposes
# table_name = "od_donation"
# schema_name = "dataverse"

# Initialize results dictionary for pipeline output
results = {
    "status": "success",
    "table_name": table_name,
    "action": "tracking_columns_processed",
    "columns_attempted": 5,
    "columns_added": 0,
    "columns_already_existed": 0,
    "message": "",
    "execution_time_seconds": 0,
    "current_column_count": 0,
    "columns_added_list": [],
    "columns_existing_list": [],
    "error_message": "",
    "error_code": ""
}

start_time = datetime.now()

try:
    # Construct full table name
    full_table_name = f"{schema_name}.{table_name}"
    
    # Step 1: Validate table exists (safety check)
    print(f"Validating table existence: {full_table_name}")
    
    tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
    table_exists = any(row['tableName'].lower() == table_name.lower() for row in tables)
    
    if not table_exists:
        results["status"] = "error"
        results["action"] = "tracking_columns_failed"
        results["error_message"] = f"Table {full_table_name} does not exist"
        results["error_code"] = "TABLE_NOT_FOUND"
        print(f"ERROR: Table {full_table_name} not found")
    else:
        # Step 2: Get current schema
        print(f"Getting current schema for {full_table_name}")
        current_schema = spark.sql(f"DESCRIBE {full_table_name}").collect()
        current_columns = [row['col_name'] for row in current_schema if row['col_name'] not in ['', '# Partitioning']]
        current_columns_lower = [col.lower() for col in current_columns]
        
        results["current_column_count"] = len(current_columns)
        print(f"Current column count: {len(current_columns)}")
        
        # Step 3: Define tracking columns to check/add (no DEFAULT values)
        tracking_columns = {
            'IsDeleted': 'boolean',
            'IsPurged': 'boolean', 
            'DeletedDate': 'timestamp',
            'PurgedDate': 'timestamp',
            'LastSynced': 'timestamp'
        }
        
        # Step 4: Check which columns need to be added
        columns_to_add = []
        columns_already_exist = []
        
        for col_name, col_type in tracking_columns.items():
            if col_name.lower() in current_columns_lower:
                columns_already_exist.append(col_name)
                print(f"✓ Column {col_name} already exists")
            else:
                columns_to_add.append(f"{col_name} {col_type}")
                print(f"+ Column {col_name} will be added")
        
        # Update results with column analysis
        results["columns_added"] = len(columns_to_add)
        results["columns_already_existed"] = len(columns_already_exist)
        results["columns_added_list"] = [col.split()[0] for col in columns_to_add]
        results["columns_existing_list"] = columns_already_exist
        
        # Step 5: Add missing columns if any
        if columns_to_add:
            print(f"Adding {len(columns_to_add)} missing tracking columns to {full_table_name}")
            
            # Build ALTER TABLE statement for only missing columns
            columns_clause = ", ".join(columns_to_add)
            alter_sql = f"ALTER TABLE {full_table_name} ADD COLUMNS ({columns_clause})"
            
            print(f"Executing SQL: {alter_sql}")
            spark.sql(alter_sql)
            
            results["action"] = "tracking_columns_added"
            results["message"] = f"Added {len(columns_to_add)} tracking columns successfully"
            print(f"✓ Successfully added {len(columns_to_add)} tracking columns")
            
        else:
            results["action"] = "tracking_columns_verified"
            results["message"] = "All tracking columns already exist"
            print("✓ All tracking columns already exist - no changes needed")
        
        # Step 6: Verify final schema
        final_schema = spark.sql(f"DESCRIBE {full_table_name}").collect()
        final_columns = [row['col_name'] for row in final_schema if row['col_name'] not in ['', '# Partitioning']]
        results["current_column_count"] = len(final_columns)
        
        print(f"Final column count: {len(final_columns)}")
        
        # Summary message
        if results["columns_added"] > 0 and results["columns_already_existed"] > 0:
            results["message"] = f"Added {results['columns_added']} new columns, {results['columns_already_existed']} already existed"
        elif results["columns_added"] > 0:
            results["message"] = f"Added {results['columns_added']} tracking columns successfully"
        else:
            results["message"] = "All tracking columns already exist"

except Exception as e:
    # Handle all errors
    error_msg = str(e)
    results["status"] = "error"
    results["action"] = "tracking_columns_failed"
    results["error_message"] = error_msg
    
    # Determine error code based on error message
    if "permission" in error_msg.lower() or "access" in error_msg.lower():
        results["error_code"] = "PERMISSION_DENIED"
    elif "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
        results["error_code"] = "TABLE_NOT_FOUND"
    elif "syntax" in error_msg.lower():
        results["error_code"] = "SQL_SYNTAX_ERROR"
    else:
        results["error_code"] = "UNKNOWN_ERROR"
    
    print(f"ERROR: {error_msg}")

finally:
    # Calculate execution time
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    results["execution_time_seconds"] = round(execution_time, 2)
    
    # Log final results
    print(f"Operation completed in {execution_time:.2f} seconds")
    print(f"Final results: {results}")

# Return results to pipeline
mssparkutils.notebook.exit(json.dumps(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
