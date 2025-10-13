# Daily Sync Pipeline with Schema Management

## Pipeline Structure

```
Activity 1: Lookup - Get Enabled Tables
Activity 2: ForEach - Process Tables
├── Activity 2.0: Notebook - Check Table Definition
├── Activity 2.1: If Condition - Table Needs to Be Created
│   ├── True Branch: Copy Activity - Create New Table (includes initial sync) 
│   │   └── Notebook - Initial Sync Logging
│   └── False Branch: Skip
├── Activity 2.2: If Condition - Table Needs Tracking Columns  
│   ├── True Branch: Notebook - Add Tracking Columns
│   │   └── Notebook - Schema Management Logging
│   └── False Branch: Skip
├── Activity 2.3: If Condition - Table Needs Daily Sync
│   ├── True Branch: Daily Sync Operations
│   │   ├── Copy Activity - Copy to Staging
│   │   ├── Notebook - Merge Staged
│   │   ├── Notebook - Drop Staging Table
│   │   ├── Copy Activity - Extract Current IDs
│   │   ├── Notebook - Delete Detection
│   │   └── Notebook - Daily Sync Logging
│   └── False Branch: Skip
└── Activity 2.4: Notebook - Final Summary (ALWAYS RUNS)
    ├── Query logged operations
    ├── Perform DataValidation
    ├── Update PipelineConfig
    ├── Create checkpoint
    └── Print comprehensive summary
```

## Architecture Overview

### Multiple Targeted Logging Approach

The pipeline uses four specialized logging notebooks called at specific execution points to eliminate race conditions in parallel ForEach execution. Each logging notebook receives parameters only from its immediate execution context, ensuring consistent and accurate logging.

### Activity Flow Description

#### Activity 1: Lookup - Get Enabled Tables
Retrieves the list of tables configured for synchronization from the metadata.PipelineConfig table.

#### Activity 2: ForEach - Process Tables
Processes each table individually with conditional branching based on table state and requirements.

#### Activity 2.0: Check Table Definition
Validates table existence, analyzes schema requirements, and determines necessary actions. Tables requiring manual creation cause immediate pipeline failure with clear error messaging.

#### Activity 2.1: Table Needs to Be Created (Conditional)
**True Branch**: Executes when a new table must be created
- **Create New Table**: Combined Copy Activity that creates the table structure and performs initial data synchronization in a single operation
- **Initial Sync Logging**: Records schema validation, table creation, and initial sync operations

#### Activity 2.2: Table Needs Tracking Columns (Conditional)  
**True Branch**: Executes when existing tables require tracking column additions
- **Add Tracking Columns**: Adds IsDeleted, IsPurged, DeletedDate, PurgedDate, and LastSynced columns with appropriate indexes
- **Schema Management Logging**: Records tracking column addition operations

#### Activity 2.3: Table Needs Daily Sync (Conditional)
**True Branch**: Executes for existing tables requiring regular data synchronization
- **Copy to Staging**: Copies current data from Dataverse to temporary staging table
- **Merge Staged**: Performs upsert operations from staging to target Bronze layer table
- **Drop Staging Table**: Removes temporary staging table to clean up resources
- **Extract Current IDs**: Copies current record identifiers for comparison purposes
- **Delete Detection**: Identifies soft deletes by comparing current IDs with Bronze layer records
- **Daily Sync Logging**: Records data synchronization and delete detection operations

#### Activity 2.4: Final Summary (Always Executes)
Performs comprehensive validation and summary operations regardless of which conditional branches executed.

## Logging Architecture

### Initial Sync Logging Notebook
**Trigger**: After Create New Table activity completion
**Operations Logged**:
- SchemaCheck: Table definition validation results
- CreateTable: New table creation operation
- InitialSync: First-time data synchronization

**Parameters**:
- table_name, schema_name, pipeline_run_id, pipeline_trigger_time
- schema_check_result, table_creation_result

### Schema Management Logging Notebook
**Trigger**: After Add Tracking Columns activity completion
**Operations Logged**:
- AddTrackingColumns: Schema enhancement operation

**Parameters**:
- table_name, schema_name, pipeline_run_id, pipeline_trigger_time
- tracking_columns_result

### Daily Sync Logging Notebook
**Trigger**: After Daily Sync Operations completion
**Operations Logged**:
- DataSync: Regular daily synchronization (upsert mode)
- DeleteDetection: Soft delete identification and classification

**Parameters**:
- table_name, schema_name, pipeline_run_id, pipeline_trigger_time
- upsert_records_result, delete_detection_result

### Final Summary Notebook
**Trigger**: Always executes at end of each table iteration
**Operations**:
- Query all logged operations for current table and pipeline run
- Reconstruct execution state from logged data
- Perform Bronze layer data validation
- Update metadata.PipelineConfig.LastDailySync
- Create checkpoint entries for successful operations
- Generate comprehensive execution summary

**Parameters**:
- table_name, schema_name, pipeline_run_id, pipeline_trigger_time

## Query-Based Reconstruction

The Final Summary notebook uses query-based reconstruction instead of complex parameter passing:

1. **Query Logged Operations**: Retrieves all operations logged for the current table and pipeline run from metadata.SyncAuditLog
2. **Reconstruct Execution State**: Determines sync type, record counts, and overall status from logged operations
3. **Validate and Update**: Performs Bronze layer validation, updates configuration, creates checkpoints, and generates summaries

## Metadata Architecture

### Schema Organization
All metadata tables use the `metadata.` schema prefix:
- metadata.SyncAuditLog
- metadata.DataValidation
- metadata.PipelineConfig
- metadata.CheckpointHistory

### SyncAuditLog Operations
- **SchemaCheck**: Table definition validation
- **CreateTable**: New table creation
- **InitialSync**: First-time table synchronization (append mode)
- **AddTrackingColumns**: Schema enhancement operations
- **DataSync**: Regular daily synchronization (upsert mode)
- **DeleteDetection**: Soft delete detection and classification
- **ParameterValidation**: Parameter error logging

### DataValidation Process
Comprehensive Bronze layer validation including:
- Internal consistency checks (total = active + deleted + purged)
- Initial sync validation (copied records match active records)
- Data integrity verification (no negative counts)
- Sync operation consistency validation with tolerance thresholds

### PipelineConfig Management
Tracks LastDailySync timestamps for successfully synchronized tables, enabling incremental processing and operational monitoring.

### Checkpoint Strategy
Creates daily checkpoints for successful operations with:
- 7-day retention for daily checkpoints
- Validation status tracking
- Total row counts for capacity planning

## Error Handling

### JSON Parsing
Robust JSON parsing with specific error identification for each parameter, including handling of empty strings, null values, and malformed JSON.

### Timestamp Processing
Timezone-aware timestamp handling with microsecond truncation to accommodate various MS Fabric timestamp formats.

### Parameter Consistency
Standardized exitValue format for notebook activities ensures consistent parsing logic across all logging notebooks.

## Benefits

### Race Condition Elimination
Each logging notebook receives parameters only from its immediate execution context, preventing variable conflicts in parallel ForEach execution.

### Targeted Logging Efficiency
Focused logging notebooks handle only relevant operations for their execution context, reducing complexity and potential errors.

### Comprehensive Audit Trail
All operations logged with consistent pipeline_run_id correlation enable complete execution reconstruction and historical analysis.

### Robust Error Recovery
Final Summary notebook functions even when conditional logging notebooks fail, ensuring consistent validation and summary generation.

### Operational Visibility
Complete audit trail with reconstructed metrics provides full visibility into pipeline execution across all conditional paths.

## Implementation Guidelines

### Development Approach
1. Create four specialized logging notebooks with focused responsibilities
2. Implement pipeline structure with conditional logging calls at appropriate execution points
3. Validate each execution path independently
4. Verify Final Summary reconstruction accuracy
5. Test complete audit trail correlation

### Parameter Management
Use direct activity output references within conditional branches and avoid complex cross-branch parameter passing. Final Summary notebook reconstruction eliminates dependency on complex parameter coordination.

### Testing Strategy
Validate all execution scenarios:
- New table creation with initial sync
- Existing table schema management
- Daily synchronization with delete detection
- Parallel table processing without parameter contamination

### Monitoring
Query metadata.SyncAuditLog for operational intelligence including success rates, processing times, and error patterns across all pipeline executions.