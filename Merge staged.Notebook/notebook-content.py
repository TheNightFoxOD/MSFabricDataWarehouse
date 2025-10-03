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

# PARAMETERS CELL ********************

table_name = "PARAM_NOT_SET_table_name"
schema_name = "PARAM_NOT_SET_schema_name"
primary_key_column = "PARAM_NOT_SET_primary_key_column"
lookback_range = 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime

###############################################
# table_name = "od_donation"
# schema_name = "dataverse"
# primary_key_column = "od_donationid"
# lookback_range = 7
###############################################

try:
    # Build table names
    target_table = f"Master_Bronze.{schema_name}.{table_name}"
    staging_table = f"dataverse_opendoorsmas_cds2_workspace_org42a53679.dbo.{table_name}"

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
    staging_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {staging_table} where {primary_key_column} is not null").collect()[0]['cnt']
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
        WHEN MATCHED AND staging.modifiedon > date_sub(current_date(), {lookback_range}) THEN 
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED AND {primary_key_column} is not null THEN INSERT 
            ({insert_columns_str})
            VALUES ({insert_values_str})
        """

        print("Executing MERGE SQL:")
        print("-" * 60)
        print(merge_sql)
        print("-" * 60)

        # Execute the merge
        spark.sql(merge_sql)

        # Get record counts after MERGE for metrics
        # Count how many records in staging are actually "modified" within lookback range
        modified_count = spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM {staging_table} 
            WHERE modifiedon > date_sub(current_date(), {lookback_range})
            AND {primary_key_column} is not null
        """).collect()[0]['count']
        target_count_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]['cnt']

        # Calculate metrics
        insert_count = target_count_after - target_count_before
        update_count = min(target_count_before, modified_count) - insert_count  # Can't update more than exist
        total_upsert_count = insert_count + update_count

        print(f"MERGE completed successfully:")
        print(f"- Target table records before merge: {target_count_before}")
        print(f"- Target table records after merge: {target_count_after}")
        print(f"- Staging table records: {staging_count}")
        print(f"- Inserted: {insert_count} records")
        print(f"- Updated: {update_count} records")
        print(f"- Total processed: {total_upsert_count} records")

        # Prepare success result
        result = {
            "status": "success",
            "upsert_count": total_upsert_count,
            "insert_count": insert_count,
            "update_count": update_count,
            "staging_records": staging_count,
            "target_table_records_before_merge": target_count_before,
            "target_table_records_after_merge": target_count_after,
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
