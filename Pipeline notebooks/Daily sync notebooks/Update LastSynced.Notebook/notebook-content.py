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

# assignment for testing purposes
# table_name = "od_donation"
# schema_name = "dataverse"

try:
    full_table_name = f"{schema_name}.{table_name}"
    
    # First, check how many records have LastSynced unset (should be all after initial load)
    unset_count_before = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM {full_table_name} 
        WHERE LastSynced IS NULL
    """).collect()[0]['count']
    
    print(f"Records with LastSynced unset before update: {unset_count_before}")
    
    # Update LastSynced for records where it's currently NULL
    update_sql = f"""
    UPDATE {full_table_name}
    SET LastSynced = current_timestamp()
    WHERE LastSynced IS NULL
    """
    
    print(f"Updating LastSynced for records with NULL values in {table_name}")
    spark.sql(update_sql)
    
    # Verify that no records have LastSynced unset after update
    unset_count_after = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM {full_table_name} 
        WHERE LastSynced IS NULL
    """).collect()[0]['count']
    
    # Get total record count for validation
    total_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
    
    # Validation: there should be no unset LastSynced records after initial sync
    validation_passed = (unset_count_after == 0 and unset_count_before > 0)
    
    result = {
        "status": "success",
        "table_name": table_name,
        "total_records": total_count,
        "records_updated": unset_count_before,
        "unset_records_remaining": unset_count_after,
        "validation_passed": validation_passed,
        "action": "lastsync_updated"
    }
    
    if not validation_passed:
        print(f"WARNING: Validation failed for {table_name}")
        if unset_count_after > 0:
            result = {"status": "error", "error_message": f" {unset_count_after} records still have LastSynced unset"}
            print(f"  - {unset_count_after} records still have LastSynced unset")
        if unset_count_before == 0:
            print(f"  - No records were found with LastSynced unset (unexpected for initial sync)")
    
    print(f"LastSynced update completed for initial sync: {result}")
    
except Exception as e:
    result = {"status": "error", "error_message": str(e)}
    print(f"Error updating LastSynced after initial sync: {e}")

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
