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

table_name = "PARAM_NOT_SET_table_name"
schema_name = "PARAM_NOT_SET_schema_name"
primary_key_column = "PARAM_NOT_SET_primary_key_column"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime

try:
    # Build table names
    target_table = f"{schema_name}.{table_name}"
    staging_table = f"temp.{table_name}_staging"

    print(f"Starting MERGE operation:")
    print(f"- Target table: {target_table}")
    print(f"- Staging table: {staging_table}")
    print(f"- Primary key: {primary_key_column}")

    # Verify both tables exist
    target_exists = spark.catalog.tableExists(target_table)
    staging_exists = spark.catalog.tableExists(staging_table)

    if not target_exists:
        raise Exception(f"Target table {target_table} does not exist")

    if not staging_exists:
        raise Exception(f"Staging table {staging_table} does not exist")

    # Get row count from staging table for metrics
    staging_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {staging_table}").collect()[0]['cnt']
    print(f"Staging table contains {staging_count} records")

    if staging_count == 0:
        print("No records in staging table - skipping MERGE operation")
        result = {
            "status": "success",
            "upsert_count": 0,
            "insert_count": 0,
            "update_count": 0,
            "table_name": table_name,
            "message": "No records to process"
        }
    else:
        # Get record counts before MERGE for metrics
        target_count_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]['cnt']
        print(f"Target table contains {target_count_before} records before MERGE")

        # Get existing table schema from lakehouse
        current_schema = spark.sql(f"DESCRIBE {target_table}").collect()

        # Extract column names, excluding our tracking columns
        tracking_columns = ['isdeleted', 'ispurged', 'deleteddate', 'purgeddate', 'lastsynced', 'syncdate']

        source_columns = [
            row['col_name'] for row in current_schema 
            if row['col_name'] not in ['', '# Partitioning'] 
            and row['col_name'].lower() not in tracking_columns
        ]

        # Build the UPDATE SET clause (target.col = staging.col for each column)
        update_set_clause = ', '.join([f"target.{col} = staging.{col}" for col in source_columns])
        update_set_clause += ", target.LastSynced = current_timestamp()"

        # Build the INSERT clause (column names and VALUES)
        insert_columns = source_columns + ['LastSynced']
        insert_columns_str = ', '.join(insert_columns)
        insert_values = [f"staging.{col}" for col in source_columns] + ['current_timestamp()']
        insert_values_str = ', '.join(insert_values)

        # Execute MERGE operation with explicit CAST for GUID handling
        merge_sql = f"""
        MERGE INTO {target_table} as target
        USING {staging_table} as staging
        ON CAST(target.{primary_key_column} AS STRING) = CAST(staging.{primary_key_column} AS STRING)
        WHEN MATCHED THEN UPDATE SET 
            {update_set_clause}
        WHEN NOT MATCHED THEN INSERT 
            ({insert_columns_str})
            VALUES ({insert_values_str})
        """

        print("Executing MERGE SQL:")
        print(merge_sql)

        # Execute the merge
        spark.sql(merge_sql)

        # Get record counts after MERGE for metrics
        target_count_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]['cnt']

        # Calculate metrics
        insert_count = target_count_after - target_count_before
        update_count = staging_count - insert_count
        total_upsert_count = insert_count + update_count

        print(f"MERGE completed successfully:")
        print(f"- Inserted: {insert_count} records")
        print(f"- Updated: {update_count} records")
        print(f"- Total processed: {total_upsert_count} records")

        # Clean up staging table
        spark.sql(f"TRUNCATE TABLE {staging_table}")
        print(f"Staging table {staging_table} truncated")

        # Prepare success result
        result = {
            "status": "success",
            "upsert_count": total_upsert_count,
            "insert_count": insert_count,
            "update_count": update_count,
            "staging_records": staging_count,
            "table_name": table_name,
            "target_table": target_table,
            "staging_table": staging_table,
            "primary_key": primary_key_column,
            "execution_time": datetime.utcnow().isoformat(),
            "message": f"Successfully processed {total_upsert_count} records ({insert_count} inserts, {update_count} updates)"
        }

except Exception as e:
    error_message = str(e)
    print(f"ERROR in MERGE operation: {error_message}")

    # Prepare error result
    result = {
        "status": "error",
        "error_message": error_message,
        "upsert_count": 0,
        "insert_count": 0,
        "update_count": 0,
        "table_name": table_name,
        "target_table": f"{target_table}",
        "staging_table": f"{staging_table}",
        "primary_key": primary_key_column,
        "execution_time": datetime.utcnow().isoformat()
    }

print("Final result:")
print(json.dumps(result, indent=2))

# Output result for pipeline consumption
mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
