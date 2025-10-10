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

table_names = "table_names_default"
purge_date = "purge_date_default"
pipeline_run_id = "pipeline_run_id_default"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Purge Parameters Notebook
# Purpose: Pre-flight validation before purge metadata update

import json
from datetime import datetime
from pyspark.sql.functions import col

# ==========================================
# STEP 1: Parse Input Parameters
# ==========================================
# Parameters are already defined in the parameters cell above
# The pipeline will inject actual values, replacing the defaults

print(f"Validation started for pipeline run: {pipeline_run_id}")
print(f"Tables to validate: {table_names}")
print(f"Purge date: {purge_date}")

# Parse table names from comma-separated string
if not table_names or table_names.strip() == "":
    raise ValueError("Parameter 'table_names' is required and cannot be empty")

table_list = [t.strip() for t in table_names.split(',') if t.strip()]

if len(table_list) == 0:
    raise ValueError("No valid table names provided")

print(f"Parsed {len(table_list)} tables: {table_list}")

# ==========================================
# STEP 2: Validate Purge Date
# ==========================================
if not purge_date or purge_date.strip() == "":
    raise ValueError("Parameter 'purge_date' is required and cannot be empty")

try:
    # Parse purge date - handle both ISO format and simplified formats
    # Remove 'Z' suffix and timezone info to make comparison easier
    purge_date_clean = purge_date.replace('Z', '').split('+')[0].split('.')[0]  # Remove Z, timezone, and microseconds
    purge_dt = datetime.fromisoformat(purge_date_clean)
    
    print(f"Purge date parsed successfully: {purge_dt}")
    
    # Check if purge date is in the future (both are now timezone-naive)
    now = datetime.utcnow()
    if purge_dt > now:
        raise ValueError(f"Purge date {purge_dt} cannot be in the future. Current UTC time: {now}")
    
    print("✓ Purge date validation passed")
    
except ValueError as ve:
    raise ValueError(f"Invalid purge date format: {purge_date}. Error: {str(ve)}")

# ==========================================
# STEP 3: Validate Tables Exist in Bronze Layer
# ==========================================
print("\nValidating table existence in Bronze layer...")
validation_errors = []
validated_tables = []

for table_name in table_list:
    try:
        # First, get schema from metadata.PipelineConfig
        schema_df = spark.sql(f"""
            SELECT SchemaName 
            FROM metadata.PipelineConfig 
            WHERE TableName = '{table_name}'
        """)
        
        if schema_df.count() == 0:
            error_msg = f"No configuration found for table '{table_name}' in metadata.PipelineConfig"
            print(f"✗ {error_msg}")
            validation_errors.append(error_msg)
            continue
        
        schema_name = schema_df.collect()[0]['SchemaName']
        full_table_name = f"{schema_name}.{table_name}"
        
        print(f"\nValidating: {full_table_name}")
        
        # Check if table exists
        table_exists = spark.catalog.tableExists(full_table_name)
        
        if not table_exists:
            error_msg = f"Table '{full_table_name}' not found"
            print(f"✗ {error_msg}")
            validation_errors.append(error_msg)
            continue
        
        print(f"✓ Table '{full_table_name}' exists")
        
        # Get table row count
        row_count = spark.table(full_table_name).count()
        print(f"  Total row count: {row_count:,}")
        
        # Estimate records that could be marked as purged based on purge date
        # These are records created before purge date that are not already purged/deleted
        try:
            estimated_purged_df = spark.sql(f"""
                SELECT COUNT(*) as count
                FROM {full_table_name}
                WHERE CreatedOn < TIMESTAMP'{purge_date_clean}'
                AND COALESCE(IsPurged, false) = false
                AND COALESCE(IsDeleted, false) = false
            """)
            
            estimated_purged_count = estimated_purged_df.collect()[0]['count']
            
            # Get count of records already marked as purged
            already_purged_df = spark.sql(f"""
                SELECT COUNT(*) as count
                FROM {full_table_name}
                WHERE COALESCE(IsPurged, false) = true
            """)
            already_purged_count = already_purged_df.collect()[0]['count']
            
            # Get count of records already marked as deleted
            already_deleted_df = spark.sql(f"""
                SELECT COUNT(*) as count
                FROM {full_table_name}
                WHERE COALESCE(IsDeleted, false) = true
            """)
            already_deleted_count = already_deleted_df.collect()[0]['count']
            
            # Calculate percentages
            purge_percentage = (estimated_purged_count / row_count * 100) if row_count > 0 else 0
            
            print(f"  Records created before {purge_date_clean}: {estimated_purged_count:,} ({purge_percentage:.1f}%)")
            print(f"  Already marked as purged: {already_purged_count:,}")
            print(f"  Already marked as deleted: {already_deleted_count:,}")
            print(f"  → Potential impact: {estimated_purged_count:,} records could be marked as purged if missing from source")
            
        except Exception as est_error:
            # If estimation fails (e.g., CreatedOn column doesn't exist), log warning but continue
            print(f"  ⚠ Could not estimate purge impact: {str(est_error)}")
            estimated_purged_count = None
            already_purged_count = None
            already_deleted_count = None
            purge_percentage = None
        
        validated_tables.append({
            "table_name": table_name,
            "schema_name": schema_name,
            "full_table_name": full_table_name,
            "exists": True,
            "row_count": row_count,
            "estimated_purged_count": estimated_purged_count,
            "already_purged_count": already_purged_count,
            "already_deleted_count": already_deleted_count,
            "purge_percentage": purge_percentage
        })
        
    except Exception as e:
        error_msg = f"Error validating table '{table_name}': {str(e)}"
        print(f"✗ {error_msg}")
        validation_errors.append(error_msg)

# ==========================================
# STEP 4: Validate Metadata Records Exist
# ==========================================
print("\nValidating metadata.PipelineConfig records...")

for table_info in validated_tables:
    table_name = table_info["table_name"]
    
    try:
        # Check if metadata record exists (already checked in STEP 3, but verify details)
        metadata_df = spark.sql(f"""
            SELECT 
                TableName,
                SchemaName,
                LastPurgeDate,
                LastDailySync
            FROM metadata.PipelineConfig
            WHERE TableName = '{table_name}'
        """)
        
        if metadata_df.count() == 0:
            error_msg = f"No configuration found for table '{table_name}' in metadata.PipelineConfig"
            print(f"✗ {error_msg}")
            validation_errors.append(error_msg)
            continue
        
        # Get current LastPurgeDate value
        current_record = metadata_df.collect()[0]
        current_purge_date = current_record['LastPurgeDate']
        
        print(f"✓ Metadata record exists for '{table_name}'")
        print(f"  Schema: {current_record['SchemaName']}")
        print(f"  Current LastPurgeDate: {current_purge_date}")
        
        table_info["metadata_exists"] = True
        table_info["current_purge_date"] = str(current_purge_date) if current_purge_date else None
        
    except Exception as e:
        error_msg = f"Error checking metadata for table '{table_name}': {str(e)}"
        print(f"✗ {error_msg}")
        validation_errors.append(error_msg)

# ==========================================
# STEP 5: Generate Impact Report
# ==========================================
print("\nGenerating impact report...")

impact_report = {
    "tables_validated": len(validated_tables),
    "validation_passed": len(validation_errors) == 0,
    "validation_errors": validation_errors,
    "table_details": validated_tables,
    "purge_date": purge_date,
    "pipeline_run_id": pipeline_run_id
}

print(f"\nValidation Summary:")
print(f"  Tables validated: {len(validated_tables)}")
print(f"  Validation errors: {len(validation_errors)}")

# ==========================================
# STEP 6: Return Validation Result
# ==========================================
if len(validation_errors) > 0:
    print("\n✗ VALIDATION FAILED")
    print("Errors:")
    for error in validation_errors:
        print(f"  - {error}")
    
    # Log validation failure
    spark.sql(f"""
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, RowsProcessed, RowsDeleted, RowsPurged,
            Status, ErrorMessage, Notes, RetryCount, CreatedDate
        ) VALUES (
            '{pipeline_run_id}_validation_failed',
            '{pipeline_run_id}',
            'PurgeMetadataUpdate',
            'Multiple',
            'PurgeValidation',
            current_timestamp(),
            current_timestamp(),
            0, 0, 0,
            'Error',
            '{"; ".join(validation_errors[:3])}',
            NULL,
            0,
            current_timestamp()
        )
    """)
    
    exit_value = {
        "validation_status": "Failed",
        "error_count": len(validation_errors),
        "errors": validation_errors
    }
    
    mssparkutils.notebook.exit(json.dumps(exit_value))
    raise Exception(f"Validation failed with {len(validation_errors)} error(s)")

else:
    print("\n✓ VALIDATION PASSED")
    
    # Log successful validation
    spark.sql(f"""
        INSERT INTO metadata.SyncAuditLog (
            LogId, PipelineRunId, PipelineName, TableName, Operation,
            StartTime, EndTime, RowsProcessed, RowsDeleted, RowsPurged,
            Status, ErrorMessage, Notes, RetryCount, CreatedDate
        ) VALUES (
            '{pipeline_run_id}_validation_success',
            '{pipeline_run_id}',
            'PurgeMetadataUpdate',
            'Multiple',
            'PurgeValidation',
            current_timestamp(),
            current_timestamp(),
            0, 0, 0,
            'Success',
            NULL,
            'All tables validated successfully',
            0,
            current_timestamp()
        )
    """)
    
    exit_value = {
        "validation_status": "Success",
        "tables_validated": len(validated_tables),
        "impact_report": impact_report
    }
    
    print(f"\nReturning: {json.dumps(exit_value, indent=2)}")
    mssparkutils.notebook.exit(json.dumps(exit_value))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
