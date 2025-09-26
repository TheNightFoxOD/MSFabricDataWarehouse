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

schema_name = "default_schema_name"
table_name = "default_table_name"
primary_key = "default_primary_key"
last_purge_date = "" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, lit, coalesce
from pyspark.sql.types import BooleanType, TimestampType

# ==========================================
# PARAMETER CELL - Mark this as parameter cell in Fabric
# ==========================================
# table_name = "od_donation"  # Will be passed as @{item().TableName}
# primary_key = "od_donationid"  # Will be passed as @{item().PrimaryKeyColumn}
# last_purge_date = ""  # Will be passed as @{item().LastPurgeDate} - can be empty string
# schema_name = "dataverse"  # Will be passed as @{item().SchemaName}
# ==========================================
# MAIN PROCESSING LOGIC
# ==========================================

def purge_aware_delete_detection():
    """
    Implements purge-aware delete detection logic:
    1. Compare current IDs from Dataverse with Bronze layer records
    2. Classify missing records as either Deleted or Purged based on LastPurgeDate
    3. Update tracking columns appropriately
    4. Clean up temporary tables
    """
    
    try:
        # Initialize variables
        full_table_name = f"{schema_name}.{table_name}"
        temp_table = f"temp.{table_name}_CurrentIDs"
        
        print(f"Starting purge-aware detection for table: {full_table_name}")
        print(f"Primary key: {primary_key}")
        print(f"Last purge date: {last_purge_date}")
        
        # ==========================================
        # STEP 1: Load temp table with current IDs
        # ==========================================
        try:
            temp_df = spark.table(temp_table)
            current_id_count = temp_df.count()
            print(f"✓ Temp table {temp_table} found with {current_id_count} current IDs")
        except Exception as e:
            raise Exception(f"Temp table {temp_table} not found. Ensure Copy Activity completed successfully: {str(e)}")
        
        # ==========================================
        # STEP 2: Load Bronze layer table
        # ==========================================
        try:
            bronze_df = spark.table(full_table_name)
            total_bronze_records = bronze_df.count()
            print(f"✓ Bronze table {full_table_name} loaded with {total_bronze_records} total records")
        except Exception as e:
            raise Exception(f"Failed to load Bronze table {full_table_name}: {str(e)}")
        
        # ==========================================
        # STEP 3: Identify missing records
        # ==========================================
        
        # Get records that exist in Bronze but not in current Dataverse
        missing_records_df = bronze_df.join(
            temp_df, 
            bronze_df[primary_key] == temp_df[primary_key], 
            "left_anti"  # Records in bronze_df that are NOT in temp_df
        )
        
        missing_count = missing_records_df.count()
        print(f"✓ Found {missing_count} records missing from Dataverse")
        
        if missing_count == 0:
            print("No missing records found - all Bronze records exist in Dataverse")
            result = {
                "status": "success",
                "table_name": table_name,
                "total_records": total_bronze_records,
                "missing_records": 0,
                "newly_deleted": 0,
                "newly_purged": 0,
                "message": "No missing records detected"
            }
            return result
        
        # ==========================================
        # STEP 4: Apply purge-aware classification logic
        # ==========================================
        
        # Parse last_purge_date if provided
        if last_purge_date and last_purge_date.strip() != "":
            try:
                # Convert string to timestamp for comparison
                purge_date_lit = lit(last_purge_date).cast(TimestampType())
                has_purge_date = True
                print(f"✓ Using purge date for classification: {last_purge_date}")
            except:
                print(f"⚠ Invalid purge date format: {last_purge_date}. Treating all missing records as deleted.")
                has_purge_date = False
                purge_date_lit = None
        else:
            has_purge_date = False
            purge_date_lit = None
            print("ℹ No purge date provided. All missing records will be classified as deleted.")
        
        # ==========================================
        # STEP 5: Update Bronze table with classification
        # ==========================================
        
        # Get current timestamp as string for proper JSON serialization
        current_ts_str = spark.sql("SELECT CAST(current_timestamp() AS STRING)").collect()[0][0]
        current_ts = lit(current_ts_str).cast(TimestampType())
        
        # Create update logic for missing records
        missing_ids = [row[primary_key] for row in missing_records_df.collect()]
        
        if has_purge_date:
            # Complex logic: check creation date against purge date
            update_df = bronze_df.withColumn(
                "IsDeleted",
                when(
                    # Record is missing from current IDs AND created after purge date
                    col(primary_key).isin(missing_ids) &
                    (col("CreatedDate") >= purge_date_lit),
                    lit(True)  # Recent missing record = deleted
                ).otherwise(col("IsDeleted"))  # Keep existing value
            ).withColumn(
                "IsPurged", 
                when(
                    # Record is missing from current IDs AND created before purge date AND not already purged
                    col(primary_key).isin(missing_ids) &
                    (col("CreatedDate") < purge_date_lit) &
                    (coalesce(col("IsPurged"), lit(False)) == lit(False)),
                    lit(True)  # Old missing record = purged
                ).otherwise(col("IsPurged"))  # Keep existing value
            ).withColumn(
                "DeletedDate",
                when(
                    # Set DeletedDate for newly deleted records
                    col(primary_key).isin(missing_ids) &
                    (col("CreatedDate") >= purge_date_lit) &
                    col("DeletedDate").isNull(),
                    current_ts
                ).otherwise(col("DeletedDate"))
            ).withColumn(
                "PurgedDate",
                when(
                    # Set PurgedDate for newly purged records
                    col(primary_key).isin(missing_ids) &
                    (col("CreatedDate") < purge_date_lit) &
                    (coalesce(col("IsPurged"), lit(False)) == lit(False)) &
                    col("PurgedDate").isNull(),
                    current_ts
                ).otherwise(col("PurgedDate"))
            )
        else:
            # Simple logic: all missing records are deleted
            update_df = bronze_df.withColumn(
                "IsDeleted",
                when(
                    col(primary_key).isin(missing_ids),
                    lit(True)  # All missing records = deleted
                ).otherwise(col("IsDeleted"))
            ).withColumn(
                "DeletedDate",
                when(
                    col(primary_key).isin(missing_ids) &
                    col("DeletedDate").isNull(),
                    current_ts
                ).otherwise(col("DeletedDate"))
            )
        
        # Update LastSynced for all records
        final_df = update_df.withColumn("LastSynced", current_ts)
        
        # ==========================================
        # STEP 6: Write updated data back to Bronze table
        # ==========================================
        
        print("✓ Applying updates to Bronze table...")
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)
        
        print("✓ Bronze table updated successfully")
        
        # ==========================================
        # STEP 7: Calculate metrics for reporting
        # ==========================================
        
        updated_df = spark.table(full_table_name)
        
        # Get comprehensive metrics
        total_records = updated_df.count()
        all_deleted_count = updated_df.filter(col("IsDeleted") == True).count()
        all_purged_count = updated_df.filter(col("IsPurged") == True).count()
        active_count = updated_df.filter(
            (col("IsDeleted") == False) & (col("IsPurged") == False)
        ).count()
        
        # Calculate records affected in this execution (for logging)
        if has_purge_date:
            deleted_records = updated_df.filter(
                (col("IsDeleted") == True) & 
                (col("DeletedDate") == current_ts)
            ).count()
            
            purged_records = updated_df.filter(
                (col("IsPurged") == True) & 
                (col("PurgedDate") == current_ts)
            ).count()
        else:
            deleted_records = updated_df.filter(
                (col("IsDeleted") == True) & 
                (col("DeletedDate") == current_ts)
            ).count()
            purged_records = 0
        
        # ==========================================
        # STEP 8: Clean up temporary table
        # ==========================================
        try:
            spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
            print(f"✓ Temporary table {temp_table} cleaned up")
        except Exception as e:
            print(f"⚠ Warning: Could not clean up temp table {temp_table}: {str(e)}")
        
        # ==========================================
        # STEP 9: Prepare results
        # ==========================================
        
        result = {
            "status": "success",
            "table_name": table_name,
            "total_records": total_records,
            "active_records": active_count,
            "deleted_records": deleted_records,           # Records deleted in THIS execution
            "purged_records": purged_records,             # Records purged in THIS execution  
            "all_deleted_records": all_deleted_count,     # Total deleted records (historical)
            "all_purged_records": all_purged_count,       # Total purged records (historical)
            "missing_from_source": missing_count,
            "has_purge_date": has_purge_date,
            "last_purge_date": last_purge_date if has_purge_date else None,
            "sync_timestamp": current_ts_str,
            "message": f"Successfully processed {missing_count} missing records"
        }
        
        print(f"✓ Purge-aware detection completed successfully")
        print(f"  - Total records: {total_records}")
        print(f"  - Active records: {active_count}")
        print(f"  - All deleted records: {all_deleted_count} (deleted in this run: {deleted_records})")
        print(f"  - All purged records: {all_purged_count} (purged in this run: {purged_records})")
        
        return result
        
    except Exception as e:
        error_msg = f"Error in purge-aware detection: {str(e)}"
        print(f"❌ {error_msg}")
        
        result = {
            "status": "error",
            "table_name": table_name,
            "error_message": error_msg,
            "error_type": type(e).__name__
        }
        
        return result

# ==========================================
# EXECUTE MAIN LOGIC
# ==========================================

# Execute the purge-aware detection
execution_result = purge_aware_delete_detection()

# Output results for pipeline consumption
print("\n" + "="*50)
print("FINAL RESULTS:")
print(json.dumps(execution_result, indent=2))
print("="*50)

# Exit with results for pipeline
mssparkutils.notebook.exit(json.dumps(execution_result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
