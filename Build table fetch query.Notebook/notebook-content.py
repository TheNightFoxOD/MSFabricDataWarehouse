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
last_daily_sync = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime, timedelta

# Build FetchXML query - fail fast, no fallback
full_table_name = f"{schema_name}.{table_name}"

print(f"Reading schema for: {full_table_name}")

# Get existing table schema from lakehouse
current_schema = spark.sql(f"DESCRIBE {full_table_name}").collect()

# Extract column names, excluding our tracking columns
tracking_columns = ['isdeleted', 'ispurged', 'deleteddate', 'purgeddate', 'lastsynced', 'syncdate']

source_columns = [
    row['col_name'] for row in current_schema 
    if row['col_name'] not in ['', '# Partitioning'] 
    and row['col_name'].lower() not in tracking_columns
]

if not source_columns:
    raise Exception(f"No source columns found for table {table_name}")

print(f"Found {len(source_columns)} source columns to sync: {source_columns}")

# Build attribute elements for FetchXML
attribute_elements = []
for column in source_columns:
    attribute_elements.append(f'    <attribute name="{column}" />')

attributes_xml = "\n".join(attribute_elements)

# Build incremental filter if we have a last sync date
filter_xml = ""
has_incremental_filter = False

if last_daily_sync and last_daily_sync.strip() and last_daily_sync != "null":
    print(f"Processing last sync date: {last_daily_sync}")
    
    # Parse last sync date and add 6-hour buffer
    if 'T' in last_daily_sync:
        # ISO format: 2024-01-15T10:00:00Z or 2024-01-15T10:00:00
        last_sync_dt = datetime.fromisoformat(last_daily_sync.replace('Z', '+00:00'))
    else:
        # Standard format: 2024-01-15 10:00:00
        last_sync_dt = datetime.strptime(last_daily_sync, '%Y-%m-%d %H:%M:%S')
    
    # Add 6-hour buffer to catch any missed records
    filter_date = last_sync_dt - timedelta(hours=6)
    filter_date_str = filter_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    filter_xml = f'''
    <filter type="and">
      <condition attribute="modifiedon" operator="on-or-after" value="{filter_date_str}" />
    </filter>'''
    
    has_incremental_filter = True
    print(f"Applying incremental filter from: {filter_date_str} (6-hour buffer)")
else:
    print("No last sync date - performing full sync")

# Build the complete FetchXML query
fetchxml_query = f'''<fetch>
  <entity name="{table_name}">
{attributes_xml}{filter_xml}
  </entity>
</fetch>'''

# Prepare result
result = {
    "fetchxml_query": fetchxml_query,
    "attributes_count": len(source_columns),
    "source_columns": source_columns,
    "has_incremental_filter": has_incremental_filter,
    "status": "success",
    "table_name": table_name
}

print("Generated FetchXML query:")
print(fetchxml_query)
print(f"\nSummary - Attributes: {len(source_columns)}, Incremental: {has_incremental_filter}")

# Output for pipeline consumption
mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
