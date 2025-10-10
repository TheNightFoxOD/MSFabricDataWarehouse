# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Microsoft Fabric Data Warehouse project implementing a medallion architecture for nonprofit fundraising data. The system synchronizes data from Microsoft Dataverse to create a Bronze → Silver → Gold data pipeline optimized for Power BI analytics and multi-office fundraising operations.

## Architecture

### Data Flow Architecture
- **Bronze Layer**: Raw data sync from Dataverse with tracking columns (IsDeleted, IsPurged, DeletedDate, LastSynced)
- **Silver Layer**: Validated, deduplicated, and enriched data with data quality scoring
- **Gold Layer**: Star schema dimensional models optimized for Power BI reporting
- **Workbench**: Development and testing workspace with prototype schemas

### Key Components
- **Daily sync notebooks**: PySpark notebooks handling incremental data synchronization
- **Data Pipelines**: Microsoft Fabric pipelines orchestrating the sync process
- **Metadata management**: Entity definitions and pipeline configuration tables
- **Monitoring**: Performance dashboards and audit logging via SyncAuditLog

### Directory Structure
- `Daily sync notebooks/`: Core data processing notebooks (PySpark)
- `Pipelines/`: Data pipeline definitions (JSON)
- `Documentation/`: Architecture guides and implementation details
- `Metadata/`: Entity definitions and configuration files
- `Workbench/`: Development and testing artifacts
- `Storage/`: Lakehouse configurations and shortcuts
- `Monitoring/`: KQL dashboards and performance monitoring

## Development Commands

This project uses Microsoft Fabric notebooks and pipelines - there are no traditional build/test commands. Development is done through:

1. **Notebook Development**: Edit `.Notebook/notebook-content.py` files directly
2. **Pipeline Testing**: Use Fabric Studio pipeline editor and manual triggers
3. **Data Validation**: Query lakehouse tables directly in Fabric

### Key Notebooks for Development
- `Tables seed.Notebook`: Initializes pipeline configuration from entity definitions
- `Log daily sync.Notebook`: Central logging for all sync operations
- `Handle Schema Drift.Notebook`: Manages schema changes in source data
- `Merge staged.Notebook`: Core merge logic for upsert operations

## Data Synchronization Patterns

### Pipeline Configuration
The system uses a metadata-driven approach via the `pipelineconfig` table:
- `EntityName`: Dataverse table name
- `IsEnabled`: Controls which tables sync
- `LastPurgeDate`: Tracks data retention
- `PrimaryKey`: Entity's primary key column

### Tracking Columns (All Bronze Tables)
- `IsDeleted`: Soft delete flag from Dataverse
- `IsPurged`: Retention management flag
- `DeletedDate`: Timestamp of deletion
- `LastSynced`: Last successful sync timestamp

### Change Data Capture
- Uses Dataverse Link to Fabric for real-time sync (2-3 minute latency)
- Watermark-based incremental loading for custom pipelines
- Delta Lake format with deletion vectors for performance

## Development Patterns

### Error Handling
All notebooks follow consistent error handling via SyncAuditLog:
```python
# Log errors with context
audit_entry = {
    "table_name": table_name,
    "pipeline_run_id": pipeline_run_id,
    "status": "Failed",
    "error_message": str(error),
    "notes": additional_context
}
```

### Parameter Passing
Microsoft Fabric notebooks use PARAMETERS CELL for pipeline parameters - no manual parameter retrieval needed.

Common parameters:
- `table_name`: Target table being processed
- `schema_name`: Target schema (typically 'dbo')
- `pipeline_run_id`: Unique identifier for tracking
- `pipeline_trigger_time`: Execution timestamp
- `dry_run`: Boolean flag for testing ("true"/"false")

### Schema Management
- Schema drift is handled automatically via `Handle Schema Drift.Notebook`
- New columns are added with appropriate defaults
- Type changes are logged but may require manual intervention

## Key Files for Understanding the System

- `Documentation/Medallion Data Processing.md`: Complete architecture guide
- `Metadata/EntityDefinitions.json`: Dataverse entity metadata
- `Daily sync notebooks/Tables seed.Notebook/`: Pipeline initialization logic
- `Pipelines/90cfa79d-abb7-890a-4eff-f63176ab52c4.DataPipeline/`: Main sync pipeline

## Important Notes

- **Microsoft Fabric Environment**: This is Microsoft Fabric, NOT Databricks
  - Use `from notebookutils import mssparkutils` (not `dbutils`)
  - Parameters are handled via PARAMETERS CELL automatically
  - Use `mssparkutils.notebook.exit()` for pipeline orchestration
- All data processing uses Delta Lake format for ACID transactions
- The system implements nonprofit-specific data patterns (donor deduplication, household modeling, RFM scoring)
- Schema evolution is automatic but breaking changes require careful handling
- Soft deletes are preserved for audit and compliance requirements