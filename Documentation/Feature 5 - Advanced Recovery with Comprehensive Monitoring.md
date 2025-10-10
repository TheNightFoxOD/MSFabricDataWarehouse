# Feature 5 Assessment: Advanced Recovery with Comprehensive Monitoring
## Updated with Discussion Decisions and Value Judgements

---

## 1. How the Checkpoint Recovery System Works

### Conceptual Overview

The checkpoint-based recovery system provides point-in-time recovery capabilities using a combination of metadata tracking and Delta Lake's time travel features. Here's how it facilitates data recovery:

#### The Recovery Mechanism

**Checkpoints as State Snapshots:**
- Checkpoints are NOT actual data backups—they're **metadata records** that capture the state of your Bronze layer at a specific point in time
- They record: timestamp, table counts, validation status, and what tables were included
- The actual data remains in Delta Lake, which maintains its own transaction log with versioning
- **Think of it like Git tags**: The checkpoint is the tag that marks a known-good state; Delta Lake is the Git history you can rewind to

**How Recovery Works:**
1. **Delta Lake Time Travel**: Delta Lake maintains a transaction log of all changes to tables. Each transaction creates a new version.
2. **Checkpoint Metadata**: The `CheckpointHistory` table stores metadata pointing to these Delta Lake versions, making them easy to identify and restore to
3. **Restoration Process**: When you rollback to a checkpoint, the system uses Delta Lake's `RESTORE TABLE` command to revert tables to their state at the checkpoint timestamp
4. **Validation**: Pre- and post-rollback validation ensures data integrity by comparing expected vs actual record counts

#### Why This Helps Recovery

**Scenario 1 - Bad Sync:**
- Daily sync accidentally marks thousands of active records as deleted due to a bug
- You create a checkpoint **before** the bad sync runs
- After detecting the issue, you rollback to the pre-sync checkpoint
- Delta Lake restores all tables to their state before the bad data was written

**Scenario 2 - Purge Operation:**
- Before performing an annual purge in Dataverse, you create a "pre-purge" checkpoint (✅ **already implemented**)
- The purge happens, and the next sync classifies missing records as purged
- If the purge was incorrect, you can rollback to restore records that shouldn't have been purged

**Key Insight**: The checkpoint metadata makes it easy to identify "safe restore points" in Delta Lake's history without manually searching through transaction logs or timestamps.

---

## 2. Delta Lake Time Travel Capabilities and Limitations

### Configuration and Retention

Delta Lake's time travel capability is controlled by two properties:

```sql
-- How far back you can time travel (transaction log retention)
ALTER TABLE bronze.account 
SET TBLPROPERTIES ('delta.logRetentionDuration' = '30 days');  -- Default: 30 days

-- How long old data files are kept (for VACUUM operations)
ALTER TABLE bronze.account 
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '7 days');  -- Default: 7 days
```

**How It Works**:
- You can time travel back as far as `logRetentionDuration` allows (default: **30 days**)
- After 30 days, old transaction log entries are cleaned up
- Running `VACUUM` removes old data files older than `deletedFileRetentionDuration`
- **IMPORTANT**: Set retention policies BEFORE you need to time travel - you can't retroactively extend them

**Customization by Table Importance**:
```sql
-- High-value tables with longer retention (for annual purge rollbacks)
ALTER TABLE bronze.donation 
SET TBLPROPERTIES ('delta.logRetentionDuration' = '365 days');

-- Low-churn tables with shorter retention (save storage)
ALTER TABLE bronze.activitypointer 
SET TBLPROPERTIES ('delta.logRetentionDuration' = '7 days');
```

### Storage Impact

**Storage Costs**:
- Delta Lake stores only **changed data**, not full table copies
- Transaction log grows over time but is compressed
- Old data files remain until `VACUUM` is run
- **Rule of thumb**: 30-day retention ≈ 20-30% additional storage (varies by change rate)

**Highly Customizable**: Each table can have different retention policies based on its importance and change frequency.

### VACUUM Operations

**Critical Understanding**: MS Fabric does **NOT** automatically run VACUUM. You must schedule it yourself.

**What Happens Without VACUUM**:
- Old data files accumulate over time
- Storage costs increase gradually
- Time travel still works fine
- Eventually you'll have lots of unused files consuming storage

**Solution**: Create a scheduled maintenance pipeline (see SM-001 story below) that runs weekly:
```python
# Weekly maintenance
VACUUM bronze.account RETAIN 168 HOURS;  -- 7 days in hours
```

**Safety Rule**: The `VACUUM RETAIN` value should match or exceed `deletedFileRetentionDuration` to avoid breaking time travel.

---

## 3. Existing Implementation Analysis

### What's Already Built

#### ✅ Checkpoint Creation (60% Implemented)

**Location**: `Daily sync notebooks/Log Execution.Notebook`

**Current Functionality**:
- Creates daily checkpoint entries in `metadata.CheckpointHistory` after successful sync
- Includes validation checks before checkpoint creation
- Records: CheckpointId, CheckpointName (format: `bronze_backup_YYYY-MM-DD`), type, row counts, validation status
- Sets 7-day retention period
- Only creates checkpoints for successful operations without critical failures

**Code Evidence**:
```python
# From Log Execution notebook
checkpoint_entry = [(
    str(uuid.uuid4()),
    f"bronze_backup_{end_time.strftime('%Y-%m-%d')}",
    'Daily',
    end_time,
    1,  # TablesIncluded - always 1 (per-table checkpoint)
    total_processed_records,
    'Validated',
    retention_date,
    True
)]
checkpoint_df.write.format("delta").mode("append").saveAsTable("metadata.CheckpointHistory")
```

**What Works Well**:
- Automated checkpoint creation during daily sync
- Proper validation before checkpoint creation
- Consistent naming convention
- Proper error handling
- Logs processed record counts for validation

**Limitations**:
- ⚠️ Only tracks one table per checkpoint (TablesIncluded = 1) - each table processed independently
- ⚠️ No cross-table checkpoint coordination (can't restore all tables to a synchronized point in time)
- ⚠️ No retention policy enforcement mechanism (checkpoints created but never deactivated when expired)
- ⚠️ RetentionDate is set but nothing acts on it

#### ✅ Metadata Schema (100% Implemented)

**Location**: `Manual Actions/Metadata Seed Notebook`

The `metadata.CheckpointHistory` table schema is properly defined and deployed:
```sql
CREATE TABLE IF NOT EXISTS metadata.CheckpointHistory (
    CheckpointId STRING NOT NULL,
    CheckpointName STRING NOT NULL,
    CheckpointType STRING NOT NULL,
    CreatedDate TIMESTAMP NOT NULL,
    TablesIncluded INT NOT NULL,
    TotalRows BIGINT,
    ValidationStatus STRING NOT NULL,
    RetentionDate DATE NOT NULL,
    IsActive BOOLEAN NOT NULL
) USING DELTA
```

**Design Decision**: The `IsActive` flag supports soft deletion - checkpoint metadata is never physically deleted (like Git history), only marked inactive.

#### ✅ Purge Metadata Update Pipeline (100% Implemented)

**Location**: `Pipelines/Purge Metadata Update (Manual).DataPipeline/` and `Update Purge Metadata.Notebook/`

**Status**: ✅ **FULLY IMPLEMENTED** (corrected from initial assessment)

**Implemented Functionality**:
1. **Parameter Validation**: Validates table names and purge date inputs
2. **Pre-Purge Checkpoint Creation**: Creates checkpoint with 365-day retention before purge
3. **Metadata Updates**: Updates `LastPurgeDate` in `PipelineConfig` for affected tables
4. **Audit Trail**: Logs purge events to `SyncAuditLog` with operation type 'PurgeRecorded'
5. **Error Handling**: Comprehensive error handling with detailed logging
6. **Transaction Safety**: Atomic updates with rollback on failure

**Pipeline Activities**:
- Activity 1: Validate Purge Parameters (notebook)
- Activity 2: Update Purge Metadata (notebook)

This pipeline is production-ready and requires no additional development for Feature 5.

#### ❌ Manual Rollback Pipeline (0% Implemented)

**Status**: Does not exist in the repository

**Expected Functionality** (from documentation):
- Checkpoint validation and selection
- Impact analysis before rollback
- Delta Lake time travel execution (`RESTORE TABLE` commands)
- Post-rollback validation
- Audit trail updates

**Status**: Not started - this is the core missing piece for recovery capability

#### ❌ Validation Reporting (0% Implemented)

**Status**: No validation comparison or reporting mechanism exists

**Expected Functionality**:
- Pre/post rollback state comparison
- Quality metrics verification
- Detailed audit trail analysis

**Status**: Not started

#### ❌ Monitoring and Alerting (0% Implemented)

**Status**: No monitoring dashboards, health reports, or alerting mechanisms exist

**Expected Functionality**:
- Email alerts on failures
- Daily health reports
- Performance dashboards

**Status**: Not started

#### ❌ Delta Lake Maintenance Pipeline (0% Implemented)

**Status**: No scheduled VACUUM/OPTIMIZE pipeline exists

**Critical Gap Identified**: Without scheduled maintenance:
- Old data files will accumulate indefinitely
- Storage costs will grow without bound
- Expired checkpoints will never be deactivated
- Performance may degrade over time

**Status**: Not started - needs new story (SM-001)

---

## 4. Coverage Analysis: Proposed Stories vs Existing Implementation

### Overall Feature Completion: ~30% (Updated)

The initial assessment of ~15% was incorrect due to the Purge Pipeline being fully implemented.

| Component | Implementation Status | Coverage |
|-----------|---------------------|----------|
| Basic Checkpoint Creation | ✅ Implemented | 60% |
| Checkpoint Metadata Schema | ✅ Implemented | 100% |
| **Purge Metadata Pipeline** | ✅ **Fully Implemented** | **100%** |
| Retention Enforcement | ❌ Not Implemented | 0% |
| Multi-Table Coordination | ❌ Not Implemented | 0% |
| Manual Rollback Pipeline | ❌ Not Implemented | 0% |
| Validation Reporting | ❌ Not Implemented | 0% |
| Monitoring & Alerting | ❌ Not Implemented | 0% |
| VACUUM/Maintenance | ❌ Not Implemented | 0% |

### CR-001: Automated Checkpoint Creation with Validation
**Proposed Acceptance Criteria**: 
- Daily checkpoints created
- Retention policy applied
- CheckpointHistory updated

**Current Coverage**: ~60% implemented

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Daily checkpoints created | ✅ Implemented | Log Execution notebook creates checkpoints after successful sync |
| Retention policy applied | ⚠️ Partially | RetentionDate is set but no enforcement mechanism (checkpoints never deactivated) |
| CheckpointHistory updated | ✅ Implemented | Checkpoint entries properly written to metadata table |

**Gaps**:
- **No retention policy enforcement** - Old checkpoints never get marked `IsActive = false`
- **No cross-table checkpoint coordination** - Each table processed independently creates separate checkpoints
- **No checkpoint integrity validation** - No periodic checks that checkpoints are still restorable

**Decision from Discussion**: Checkpoint metadata records should be kept indefinitely (never deleted), only marked `IsActive = false` when they expire. This preserves audit history like Git tags.

---

### CR-002: Enhanced Manual Rollback Capability
**Proposed Acceptance Criteria**:
- Checkpoint selection pipeline
- Delta Lake time travel
- Comprehensive validation after rollback

**Current Coverage**: 0% implemented

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Checkpoint selection pipeline | ❌ Not implemented | No pipeline exists |
| Delta Lake time travel | ❌ Not implemented | No RESTORE TABLE logic exists |
| Comprehensive validation | ❌ Not implemented | No post-rollback validation exists |

**Gaps**: Entire story is unimplemented - this is the highest-priority missing capability

**Value Assessment**: This is the **core recovery capability** that makes all the checkpoint work valuable. Without rollback, checkpoints are just metadata records with no actionable use.

---

### CR-003: Rollback Validation with Audit Trail
**Proposed Acceptance Criteria**:
- Pre/post rollback comparison
- Quality metrics verified
- Complete audit trail maintained

**Current Coverage**: 0% implemented (audit infrastructure exists but no rollback operations)

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Pre/post rollback comparison | ❌ Not implemented | No comparison logic exists |
| Quality metrics verified | ❌ Not implemented | No quality metrics for rollback |
| Complete audit trail | ⚠️ Partial infrastructure | SyncAuditLog exists but no rollback operations logged |

**Gaps**: Entire story is unimplemented though the underlying audit table infrastructure is ready

---

### CR-004: Comprehensive Monitoring and Alerting
**Proposed Acceptance Criteria**:
- Email on failures
- Daily health reports
- Performance dashboards

**Current Coverage**: 0% implemented

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Email on failures | ❌ Not implemented | No email activities in pipelines |
| Daily health reports | ❌ Not implemented | No reporting mechanism exists |
| Performance dashboards | ❌ Not implemented | No dashboards configured |

**Gaps**: Entire story is unimplemented

**Value Assessment**: Should be split into two priorities:
- **High value** (2h): Email alerts on failures - immediate operational benefit
- **Lower urgency** (6h): Performance dashboards - valuable but not blocking

---

## 5. Multi-Table Checkpoint Coordination Explained

**Current Behavior**:
The Daily Sync Pipeline processes tables in parallel via ForEach loop. Each table gets its own checkpoint with slightly different timestamps:
- Table A checkpoint: 2025-01-15 02:15:23 (50,000 rows)
- Table B checkpoint: 2025-01-15 02:18:45 (120,000 rows)
- Table C checkpoint: 2025-01-15 02:22:11 (35,000 rows)

**Problem**:
If you need to rollback, you have 3 separate checkpoints at different times. Restoring to these creates **temporal inconsistency**:
- Donation records might reference Account IDs that don't exist at that point
- Related entities (accounts and their donations) could be out of sync
- No guarantee of referential integrity across tables

**Multi-Table Coordination Solution**:
Create ONE "batch" checkpoint after all tables complete that represents a **synchronized point in time**:
- Batch checkpoint: 2025-01-15 02:30:00 (after all tables finished)
- References all 50 tables together
- Total rows: 205,000 across all tables
- When you rollback to this checkpoint, all tables restore to this synchronized point

**Implementation Approach**:
Add a "Final Pipeline Summary" activity that runs **after** the ForEach loop completes:
1. Queries all individual table checkpoints created during this pipeline run
2. Creates one aggregate "batch" checkpoint with:
   - CheckpointType = 'DailyBatch'
   - TablesIncluded = actual count (e.g., 50)
   - TotalRows = sum across all tables
   - CreatedDate = end of pipeline execution
3. This batch checkpoint becomes the restore target for synchronized rollback

**Value**: Ensures referential integrity and data consistency when rolling back multiple related tables.

---

## 6. Validation Thresholds Discussion

**Initial Position**: "We don't have any room for error, so maybe we don't need thresholds"

**Clarification After Discussion**: Thresholds serve different purposes in different contexts.

### Zero Tolerance Validations (No Thresholds)
These must be **exactly correct** with no tolerance:
- ✅ `active + deleted + purged = total` - Mathematical identity, must equal exactly
- ✅ No null primary keys - Binary: either null or not
- ✅ Schema validation - Columns either exist or don't
- ✅ Internal consistency checks - Data integrity rules

**Decision**: Start with zero tolerance for all internal consistency checks.

### Tolerances for Temporal Operations (When Needed)
Small tolerances (1-5%) may be needed for:
- **Timing edge cases**: Checkpoint created at 02:30:00, but a few records inserted at 02:30:01
- **Checkpoint expectations vs current state**: Checkpoint says "50,000 rows" but current has 50,050 - is that corruption or just new syncs since checkpoint?
- **Restore precision**: Delta Lake time travel to "2025-01-15 02:30:00" - does it restore to exactly that microsecond or within a small window?

**Decision**: 
1. Start with zero tolerance everywhere
2. Add small tolerance (1-5%, configurable) only if we encounter false positives during testing
3. Make all tolerance thresholds configurable in metadata
4. Always flag and investigate cases that exceed tolerance, even if they "pass"

**Rationale**: Better to be too strict initially and relax if needed, than to hide real data integrity issues with overly generous tolerances.

---

## 7. Revised User Stories with Proper Acceptance Criteria

### CR-001: Automated Checkpoint Creation with Validation and Retention

**Story**: 
As a system administrator, I need automated checkpoint creation after each successful sync with enforced retention policies and multi-table coordination, so that I have reliable, synchronized recovery points without manual intervention and storage doesn't grow unbounded.

**Acceptance Criteria**:

1. **Per-Table Checkpoint Creation** (Already 60% implemented):
   - [x] Checkpoint entry created in `metadata.CheckpointHistory` after each successful table sync
   - [x] Checkpoint name follows format: `bronze_backup_{YYYY-MM-DD}`
   - [x] Checkpoint includes: timestamp, table name, total rows, validation status
   - [ ] CheckpointType supports: 'Daily', 'PrePurge' (✅ implemented in Purge pipeline), 'Manual', 'PreMaintenance', 'DailyBatch' (new)
   - [x] No checkpoint created if sync has critical failures or validation fails

2. **Multi-Table Batch Checkpoint Coordination** (NEW):
   - [ ] Daily sync creates a "batch" checkpoint entry after ForEach loop completes
   - [ ] Batch checkpoint runs in new "Final Pipeline Summary" activity after all tables processed
   - [ ] Batch checkpoint records accurate TablesIncluded count (e.g., 50 tables)
   - [ ] Batch checkpoint aggregates TotalRows across all tables
   - [ ] Batch checkpoint CheckpointType = 'DailyBatch'
   - [ ] Batch checkpoint timestamp = end of pipeline execution (synchronized point)
   - [ ] Individual per-table checkpoints retained for granular troubleshooting
   - [ ] Batch checkpoint is the **primary target** for rollback operations

3. **Validation Before Creation** (Already implemented):
   - [x] Bronze layer validation passes (active + deleted + purged = total)
   - [x] No data integrity issues detected (negative counts)
   - [x] Checkpoint only marked 'Validated' if all checks pass

4. **Retention Policy Enforcement** (NEW):
   - [ ] Scheduled maintenance pipeline (SM-001) enforces retention policies
   - [ ] Daily checkpoints: 7-day retention (configurable per table in `PipelineConfig`)
   - [ ] DailyBatch checkpoints: 7-day retention (same as daily)
   - [ ] PrePurge checkpoints: 365-day retention (✅ already set in Purge pipeline)
   - [ ] Manual checkpoints: 30-day retention (unless explicitly marked for long-term retention)
   - [ ] Automated cleanup job marks `IsActive = false` for expired checkpoints (soft delete)
   - [ ] **Checkpoint metadata never physically deleted** - preserved for audit history
   - [ ] Exception: Never deactivate checkpoints of type 'PrePurge' or 'Manual' unless RetentionDate passed

5. **Checkpoint Integrity Validation** (NEW):
   - [ ] Weekly job validates that active checkpoints are still restorable
   - [ ] Checks that Delta Lake transaction log still contains checkpoint timestamps
   - [ ] Validates checkpoint metadata matches actual Delta Lake state
   - [ ] Alerts if checkpoints become unrestorable due to expired Delta Lake retention
   - [ ] Logs validation results to `SyncAuditLog`

6. **Error Handling and Reporting**:
   - [x] Checkpoint creation failure logged to `SyncAuditLog` with error details
   - [x] Checkpoint creation failure does NOT fail the overall sync pipeline
   - [ ] Retry logic (2 attempts) for transient checkpoint creation failures
   - [ ] Alert if checkpoint creation fails for 3+ consecutive days

**Definition of Done**:
- [ ] All acceptance criteria met and tested
- [ ] Unit tests for checkpoint creation logic (including edge cases)
- [ ] Integration test: Batch checkpoint created with correct TablesIncluded count
- [ ] Integration test: Checkpoint retention enforcement working (checkpoints deactivated on schedule)
- [ ] Integration test: Checkpoint integrity validation detects unrestorable checkpoints
- [ ] Documentation updated with checkpoint creation process
- [ ] Documentation: Multi-table vs per-table checkpoints explained
- [ ] Code reviewed and merged to main branch

**Dependencies**: 
- SM-001 (Maintenance Pipeline) for retention enforcement
- Requires Delta Lake retention policies configured per table

**Estimate**: 14h (increased from 12h to include batch coordination and integrity validation)

**Priority**: High (Foundation for all recovery capabilities)

---

### CR-002: Manual Rollback Pipeline with Impact Analysis

**Story**:
As a system administrator, I need a manual rollback pipeline that allows me to select a checkpoint, view impact analysis, and restore tables to that checkpoint state, so that I can quickly recover from data corruption or incorrect sync operations with confidence and full visibility.

**Acceptance Criteria**:

1. **Pipeline Creation**:
   - [ ] New pipeline created: "Manual Rollback to Checkpoint"
   - [ ] Pipeline parameters: 
     - `CheckpointName` (string, required) - name of checkpoint to restore to
     - `CheckpointType` (dropdown: 'DailyBatch', 'PrePurge', 'Manual') - filter checkpoint selection
     - `TablesScope` (default: "All") - optional: comma-separated list of specific tables
     - `RequireApproval` (boolean, default: true) - whether manual confirmation needed
     - `DryRun` (boolean, default: true) - safety feature: preview before executing
   - [ ] Pipeline accessible from Fabric workspace pipelines list with clear naming

2. **Checkpoint Selection and Validation**:
   - [ ] Activity 1: Notebook validates checkpoint exists in `CheckpointHistory`
   - [ ] Validates checkpoint `IsActive = true` (not expired)
   - [ ] Validates checkpoint has `ValidationStatus = 'Validated'`
   - [ ] Validates checkpoint timestamp is within Delta Lake `logRetentionDuration` for all tables
   - [ ] Returns checkpoint metadata: CreatedDate, TablesIncluded, TotalRows, CheckpointType, tables list
   - [ ] If validation fails, pipeline terminates immediately with clear error message and recommended actions
   - [ ] Logs validation attempt to `SyncAuditLog` as operation type 'RollbackValidation'

3. **Impact Analysis** (NEW - emphasis on visibility):
   - [ ] Activity 2: Notebook generates comprehensive impact report
   - [ ] **For each table**: 
     - Current state: total rows, active, deleted, purged counts
     - Checkpoint state: expected total rows based on checkpoint timestamp
     - Delta: rows that will be added/removed by rollback
     - Time span: how far back in time (days/hours)
     - Risk assessment: High/Medium/Low based on data volume change
   - [ ] **Aggregate metrics**:
     - Total tables affected
     - Total data volume change (rows added/removed)
     - Estimated rollback duration
     - Storage impact estimate
   - [ ] **Dependency analysis**:
     - Identifies related tables that may need coordinated rollback
     - Warns if rolling back subset of tables may break referential integrity
   - [ ] Impact report logged to `SyncAuditLog` as operation type 'RollbackImpactAnalysis'
   - [ ] Impact report displayed in pipeline output in human-readable format
   - [ ] Report includes "continue/abort" decision point if `RequireApproval = true`

4. **Conditional Rollback Execution**:
   - [ ] If Condition: Proceeds only if validation passed AND (DryRun = false) AND (RequireApproval = false OR admin confirms)
   - [ ] True branch: Execute rollback
   - [ ] False branch: Send notification summarizing what WOULD happen and exit gracefully
   - [ ] Dry-run mode generates full impact analysis but stops before execution

5. **Delta Lake Time Travel Execution**:
   - [ ] Activity 3: Notebook executes `RESTORE TABLE` for each table in scope
   - [ ] Uses checkpoint CreatedDate as restore timestamp
   - [ ] For each table: `RESTORE TABLE {schema}.{table} TO TIMESTAMP AS OF '{checkpoint_timestamp}'`
   - [ ] **Transaction safety**: Each table restore is atomic (succeeds or fails independently)
   - [ ] **Graceful degradation**: If one table restore fails, continue with remaining tables
   - [ ] Each restore logged to `SyncAuditLog` as operation type 'TableRestore' with individual status
   - [ ] All failures logged with specific error messages including Delta Lake error details
   - [ ] Progress indicator: logs "Restoring table X of Y" during execution

6. **Post-Rollback Validation**:
   - [ ] Activity 4: Notebook validates restored data integrity
   - [ ] For each restored table:
     - Verify record counts match checkpoint expectations (zero tolerance for batch checkpoints)
     - Verify internal consistency: active + deleted + purged = total
     - Verify no null primary keys introduced
     - Verify tracking columns (IsDeleted, IsPurged) internally consistent
   - [ ] Create `DataValidation` entry for post-rollback state
   - [ ] If validation fails for any table, create CRITICAL alert entry in `SyncAuditLog`
   - [ ] Validation report includes pass/fail status per table with specific metrics

7. **Post-Rollback Checkpoint Creation**:
   - [ ] Create new checkpoint after successful rollback
   - [ ] CheckpointType = 'PostRollback'
   - [ ] Links to original checkpoint (via Notes field)
   - [ ] Captures post-rollback state for potential re-rollback if needed
   - [ ] 30-day retention for post-rollback checkpoints

8. **Audit Trail Completeness**:
   - [ ] All operations logged to `SyncAuditLog` with same PipelineRunId for traceability
   - [ ] Operations include: RollbackValidation, RollbackImpactAnalysis, TableRestore (per table), PostRollbackValidation, PostRollbackCheckpoint
   - [ ] Rollback operation metadata includes: 
     - Admin user who triggered pipeline
     - Checkpoint name being restored to
     - Tables affected (list)
     - Timestamp of rollback execution
     - Reason/notes (via pipeline parameter, optional)
   - [ ] Audit trail queryable: "Show me all rollbacks in last 30 days"
   - [ ] Audit trail includes dry-run attempts (marked as DryRun = true)

**Definition of Done**:
- [ ] All acceptance criteria met and tested
- [ ] End-to-end test: Create checkpoint → corrupt data → rollback → verify restoration
- [ ] End-to-end test: Dry-run mode generates impact analysis without executing
- [ ] Error handling test: Attempt rollback to invalid checkpoint
- [ ] Error handling test: Attempt rollback outside Delta Lake retention window
- [ ] Error handling test: Partial rollback failure (some tables succeed, some fail)
- [ ] Performance test: Rollback of 50+ tables completes within acceptable timeframe
- [ ] Documentation: Operator runbook for executing manual rollback
- [ ] Documentation: How to interpret impact analysis report
- [ ] Documentation: Troubleshooting guide for common rollback failures
- [ ] Admin training materials created (screenshots, example scenarios)
- [ ] Code reviewed and merged to main branch

**Dependencies**: 
- CR-001 (requires functional batch checkpoint creation)
- Delta Lake retention policies must be configured appropriately

**Estimate**: 24h

**Priority**: **CRITICAL** - This is the core recovery capability that makes all checkpoint infrastructure valuable

---

### CR-003: Rollback Validation and Reporting

**Story**:
As a data analyst, I need comprehensive validation reports comparing pre-rollback and post-rollback states with detailed audit trails, so that I can verify data integrity after rollback operations and understand the impact on downstream processes with quantifiable metrics.

**Acceptance Criteria**:

1. **Pre-Rollback State Capture**:
   - [ ] Notebook captures complete current state before rollback begins
   - [ ] For each table: 
     - Total rows, active rows, deleted rows, purged rows
     - Latest sync timestamp
     - Delta Lake version number
     - Sample record IDs (top 100) for validation
   - [ ] State saved to dedicated `metadata.RollbackStateSnapshots` table
   - [ ] Snapshot linked to rollback PipelineRunId for correlation

2. **Post-Rollback State Capture**:
   - [ ] Notebook captures complete state after rollback completes
   - [ ] Same metrics captured as pre-rollback for direct comparison
   - [ ] Capture Delta Lake version numbers after restore (confirms time travel occurred)
   - [ ] Sample record IDs (top 100) for validation

3. **Comparison Analysis** (Detailed Delta Report):
   - [ ] Generate comprehensive delta report showing:
     - **Records restored**: Count and percentage (records that didn't exist pre-rollback)
     - **Records removed**: Count and percentage (records that existed pre-rollback but not post)
     - **Records unchanged**: Count (for sanity check)
     - **State changes**: How many active→deleted, deleted→active, etc.
   - [ ] Calculate impact percentages: % data lost, % data restored, % data unchanged
   - [ ] **Identify unexpected records**: 
     - Records that exist post-rollback but also didn't exist at checkpoint time (anomalies)
     - Records missing that should exist based on checkpoint metadata
   - [ ] **Time travel validation**:
     - Confirm Delta Lake versions changed correctly
     - Verify timestamps align with checkpoint timestamp
   - [ ] Comparison report saved to `metadata.RollbackComparisonReports` table

4. **Quality Metrics Verification** (Zero Tolerance):
   - [ ] For each restored table:
     - ✅ Verify restored row counts match checkpoint expectations exactly (zero tolerance)
     - ✅ Verify no data corruption: no null primary keys
     - ✅ Verify internal consistency: active + deleted + purged = total
     - ✅ Verify tracking columns (IsDeleted, IsPurged) are internally consistent
   - [ ] **Referential integrity checks** (if table relationships known):
     - Verify foreign key relationships intact
     - Identify orphaned records
   - [ ] All quality checks logged to `DataValidation` table with pass/fail status
   - [ ] **Quality score calculated**: 100% = perfect, < 100% = issues detected
   - [ ] Detailed issues list generated for any failing checks

5. **Audit Trail Completeness and Queryability**:
   - [ ] Every step of rollback process logged to `SyncAuditLog` with consistent PipelineRunId
   - [ ] Log entries include: operation type, timestamp, user/pipeline, status, row counts, duration
   - [ ] Failed operations logged with full error details and stack traces
   - [ ] Chain of operations fully traceable via PipelineRunId
   - [ ] **Queryable audit trail**:
     - List all rollbacks by date range
     - List all rollbacks for specific table
     - List all failed rollbacks with reasons
     - Show rollback frequency trends
   - [ ] Audit queries documented and saved as views for easy access

6. **Validation Report Generation**:
   - [ ] Comprehensive report generated and saved to `metadata.RollbackValidationReports` table
   - [ ] Report includes:
     - **Header**: checkpoint name, rollback timestamp, triggered by, tables affected
     - **Pre/post metrics**: row counts, state distribution, Delta Lake versions
     - **Delta analysis**: what changed, impact percentages
     - **Quality assessment**: validation status (Pass/Fail), quality score, issues list
     - **Recommendations**: next steps if validation failed
   - [ ] Report format: structured table (Delta Lake) for SQL querying + JSON blob for detailed data
   - [ ] Report accessible via SQL query for analysts
   - [ ] Example queries documented: "Show validation reports for last 30 days", "Find failed validations"

7. **Alerting on Validation Failures**:
   - [ ] If validation fails (quality score < 100%), create high-priority alert entry
   - [ ] Alert includes:
     - Specific failure reasons (e.g., "Table X: 5 null PKs detected")
     - Affected tables with failure details
     - Recommended actions (e.g., "Re-rollback to earlier checkpoint", "Manual data fix required")
     - Link to detailed validation report
   - [ ] Alert triggers notification (email or Teams message) to data admin team
   - [ ] Alert severity: CRITICAL (data corruption), HIGH (validation failures), MEDIUM (warnings)
   - [ ] Alerts logged to `SyncAuditLog` with operation type 'ValidationAlert'

8. **Downstream Impact Analysis**:
   - [ ] Report includes notes field for documenting downstream process impact
   - [ ] Checklist template: "Have downstream consumers been notified?", "Do reports need refresh?"
   - [ ] Links to related tables/processes that may be affected by rollback

**Definition of Done**:
- [ ] All acceptance criteria met and tested
- [ ] Sample validation report generated and reviewed by data analysts for usability
- [ ] Query examples documented and tested (all common queries work)
- [ ] Integration test: Full rollback cycle with validation report generation
- [ ] Integration test: Validation failure triggers correct alerts
- [ ] Error handling test: Validation report generation resilient to unexpected data states
- [ ] Documentation: How to interpret validation reports (with annotated examples)
- [ ] Documentation: SQL query examples for common audit trail questions
- [ ] Documentation: Troubleshooting guide for validation failures
- [ ] Training session delivered to data analyst team
- [ ] Code reviewed and merged to main branch

**Dependencies**: 
- CR-002 (requires rollback pipeline)
- Requires `metadata.RollbackStateSnapshots` and `metadata.RollbackComparisonReports` tables created

**Estimate**: 10h (reduced from 12h, focused on validation and reporting logic)

**Priority**: Medium-High (Provides confidence and transparency for rollback operations)

---

### CR-004: Monitoring Dashboards and Alert System

**Story**:
As a DevOps engineer, I need automated monitoring dashboards and alerting for checkpoint operations, sync health, and data quality, so that I can proactively detect and respond to issues before they impact downstream processes with minimal manual monitoring effort.

**Recommendation**: Split into two phases for better prioritization:
- **Phase 1 (High Priority)**: Email alerts on failures - 2h
- **Phase 2 (Lower Priority)**: Dashboards and health reports - 6h

#### Phase 1: Email Alerts on Failures (High Priority)

**Acceptance Criteria**:

1. **Alert Infrastructure Setup**:
   - [ ] Logic App or Power Automate flow created for email notifications
   - [ ] Alert email template designed (professional, actionable, clear)
   - [ ] Distribution list configured (data admin team, DevOps team)
   - [ ] Alert severity levels defined: CRITICAL, HIGH, MEDIUM, LOW

2. **Failure Alert Triggers**:
   - [ ] Alert triggered on: 
     - Checkpoint creation failure (any table)
     - Daily sync pipeline failure (entire pipeline or multiple tables)
     - Data validation failure (DataValidation.ValidationPassed = false)
     - Rollback pipeline failure (if implemented)
     - Purge metadata update failure
   - [ ] Alert includes:
     - Failure type and severity
     - Affected tables (specific list)
     - Error message and stack trace (truncated if too long)
     - Timestamp of failure
     - Pipeline run ID for troubleshooting
     - Direct link to SyncAuditLog query for details
   - [ ] Alert email sent within 5 minutes of failure detection

3. **Alert Deduplication**:
   - [ ] Don't send duplicate alerts for same failure within 1 hour
   - [ ] Batch multiple table failures into single email (not 50 separate emails)
   - [ ] Include failure count in subject line: "[CRITICAL] 5 tables failed in Daily Sync"

4. **Actionable Content**:
   - [ ] Email includes troubleshooting links (runbook, documentation)
   - [ ] Suggested first steps: "Check SyncAuditLog for error details", "Verify source system connectivity"
   - [ ] Link to re-run pipeline (if applicable)

**Estimate**: 2h

**Priority**: **HIGH** - Immediate operational value

#### Phase 2: Health Reports and Dashboards (Lower Priority)

**Acceptance Criteria**:

1. **Daily Health Report**:
   - [ ] Scheduled pipeline runs daily at 8:00 AM (configurable)
   - [ ] Report queries `SyncAuditLog` and `DataValidation` for previous 24 hours
   - [ ] Report includes:
     - **Summary**: Total syncs, successful syncs, failed syncs, success rate %
     - **Tables processed**: Count and list
     - **Rows processed**: Total across all tables, broken down by table
     - **Checkpoint status**: How many created, any failures
     - **Validation results**: Pass rate, any failures
     - **Trend analysis**: Compare to previous day (better/worse/same)
     - **Top 5 slowest tables**: By sync duration
   - [ ] Report emailed to data admin team as formatted HTML email
   - [ ] Report also available as Power BI report (refreshed daily)

2. **Performance Monitoring Dashboard** (Power BI):
   - [ ] Dashboard created with connection to Lakehouse metadata tables
   - [ ] Dashboard refreshed hourly (or on-demand)
   - [ ] **Key Metrics Displayed**:
     - Sync duration by table (bar chart, last 30 days trend)
     - Rows processed per minute (throughput metric)
     - Error rate by table (% of failed syncs)
     - Checkpoint coverage (% of tables with recent checkpoints)
     - Data quality score (from DataValidation table)
   - [ ] **Visual Indicators**:
     - Green: All systems healthy
     - Yellow: Degraded performance (e.g., sync taking 2x normal time)
     - Red: Critical issues (failures, validation errors)
   - [ ] Dashboard accessible via Fabric workspace or embedded in SharePoint
   - [ ] Dashboard includes filters: date range, table selection, pipeline type

3. **Checkpoint Health Monitoring**:
   - [ ] Dashboard panel showing:
     - Total active checkpoints by type (Daily, DailyBatch, PrePurge, Manual)
     - Checkpoint creation trend (line chart over time)
     - Retention compliance (% of checkpoints within retention policy)
     - Checkpoint gaps (tables missing recent checkpoints)
   - [ ] Alert if no checkpoint created in last 24 hours for any table
   - [ ] Alert if checkpoint creation failure rate > 5% over 7 days
   - [ ] Visual timeline of checkpoints (easy to identify restore points)

4. **Data Quality Monitoring**:
   - [ ] Dashboard panel showing:
     - Validation pass rate by table (last 30 days)
     - Data integrity score by table (calculated from ValidationPassed)
     - Anomaly detection: sudden spikes in deleted/purged records
   - [ ] Alert if validation fails for any table
   - [ ] Alert if record count deviation > 10% from expected (configurable threshold)
   - [ ] Alert if deleted/purged ratio exceeds thresholds (configurable per table)
   - [ ] Trend chart: data quality score over time

5. **Automated Health Checks**:
   - [ ] Scheduled job runs every 4 hours to check system health
   - [ ] Queries `DataValidation` for latest validation results
   - [ ] Checks for anomalies:
     - Sudden spike in deleted records (> 2 standard deviations from mean)
     - Unexpected purged records (when no purge operation logged)
     - Checkpoint creation failures
     - Sync duration increase (> 50% slower than average)
   - [ ] Creates alert entries in `SyncAuditLog` if issues detected
   - [ ] Proactive detection of issues before they escalate

**Estimate**: 6h

**Priority**: Medium (Valuable but not blocking core functionality)

**Definition of Done** (Full CR-004):
- [ ] All acceptance criteria met for both phases
- [ ] Email alert system tested (trigger test failure to verify alert sent and received)
- [ ] Alert deduplication working (multiple failures = one email)
- [ ] Daily health report delivered successfully for 3 consecutive days
- [ ] Power BI dashboard deployed and accessible to team
- [ ] Dashboard auto-refresh working (hourly updates confirmed)
- [ ] All health check queries optimized (execute in < 5 seconds)
- [ ] Monitoring runbook documented (how to respond to different alert types)
- [ ] Dashboard user guide created (how to interpret metrics)
- [ ] False positive analysis: Ensure alerts are actionable, not noisy
- [ ] Code reviewed and merged to main branch

**Dependencies**: 
- CR-001, CR-002, CR-003 (requires full audit trail data)
- Power BI workspace access
- Email service (Logic App/Power Automate) configured

**Total Estimate CR-004**: 8h (2h + 6h)

**Priority**: Phase 1 = HIGH, Phase 2 = MEDIUM

---

### SM-001: Scheduled Delta Lake Maintenance with Storage Management

**Story**:
As a platform administrator, I need an automated weekly maintenance pipeline that performs VACUUM operations, optimizes Delta Lake tables, and enforces checkpoint retention policies, so that storage costs remain controlled, query performance stays optimal, and checkpoint metadata stays clean—all without manual intervention.

**Acceptance Criteria**:

1. **Pipeline Creation**:
   - [ ] New scheduled pipeline created: "Weekly Delta Lake Maintenance"
   - [ ] Pipeline scheduled to run every Sunday at 2:00 AM UTC (configurable via trigger)
   - [ ] Pipeline runs automatically via schedule trigger (no manual invocation required)
   - [ ] Pipeline supports manual triggering for emergency maintenance

2. **Table Configuration**:
   - [ ] Maintenance configuration added to `metadata.PipelineConfig` table
   - [ ] New columns added:
     - `VacuumRetentionHours` (INT, default: 168 = 7 days) - how long to keep old data files
     - `EnableVacuum` (BOOLEAN, default: true) - whether to vacuum this table
     - `EnableOptimize` (BOOLEAN, default: true) - whether to optimize this table
     - `OptimizeZOrderColumns` (STRING, nullable) - comma-separated columns for Z-ORDER BY
   - [ ] Per-table configuration supports different retention policies
   - [ ] Example: High-value donation table = 720 hours (30 days), low-churn table = 168 hours (7 days)

3. **Delta Lake Retention Policy Verification**:
   - [ ] Before first maintenance run, verify `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` are set appropriately per table
   - [ ] Maintenance job warns if attempting to VACUUM with retention < table's `deletedFileRetentionDuration`
   - [ ] Job validates that VACUUM retention doesn't break active checkpoint time travel capability
   - [ ] Recommendation: Set table properties before enabling maintenance

4. **VACUUM Operation**:
   - [ ] For each enabled table (EnableVacuum = true), execute: `VACUUM {schema}.{table} RETAIN {VacuumRetentionHours} HOURS`
   - [ ] VACUUM only runs if table has EnableVacuum = true
   - [ ] VACUUM respects table's `delta.deletedFileRetentionDuration` property
   - [ ] VACUUM execution logged to `SyncAuditLog` with operation type 'Vacuum'
   - [ ] Logs include:
     - Start time, end time, duration
     - Files deleted count (if available from Spark metrics)
     - Estimated storage space reclaimed (if available)
     - Status (Success/Failed)
   - [ ] Individual table VACUUM failure doesn't stop pipeline (graceful degradation)

5. **OPTIMIZE Operation**:
   - [ ] For each enabled table (EnableOptimize = true), execute: `OPTIMIZE {schema}.{table}`
   - [ ] If OptimizeZOrderColumns is set, execute: `OPTIMIZE {schema}.{table} ZORDER BY ({columns})`
   - [ ] OPTIMIZE only runs if table has EnableOptimize = true
   - [ ] OPTIMIZE execution logged to `SyncAuditLog` with operation type 'Optimize'
   - [ ] Logs include: start time, end time, duration, files added/removed (from Spark metrics)
   - [ ] Recommended Z-ORDER columns: LastSynced, IsDeleted, primary key
   - [ ] Individual table OPTIMIZE failure doesn't stop pipeline

6. **Metadata Table Maintenance**:
   - [ ] VACUUM and OPTIMIZE applied to metadata tables: 
     - `metadata.SyncAuditLog`
     - `metadata.CheckpointHistory`
     - `metadata.DataValidation`
     - `metadata.PipelineConfig`
   - [ ] Metadata tables use longer VACUUM retention (720 hours = 30 days minimum)
   - [ ] Metadata tables optimized for common query patterns (Z-ORDER BY PipelineRunId, TableName, CreatedDate)

7. **Checkpoint Retention Enforcement** (IMPORTANT):
   - [ ] Maintenance pipeline queries `CheckpointHistory` for expired checkpoints (RetentionDate < current date)
   - [ ] Updates `IsActive = false` for expired checkpoints (soft delete)
   - [ ] **Never physically delete checkpoint records** (preserved for audit history)
   - [ ] Deactivation logged to `SyncAuditLog` with operation type 'CheckpointRetentionEnforcement'
   - [ ] **Exception handling**:
     - Never deactivate checkpoints of type 'PrePurge' until RetentionDate passed (365 days default)
     - Never deactivate checkpoints of type 'Manual' unless explicitly marked for deletion
     - Never deactivate checkpoints that are still within Delta Lake time travel window
   - [ ] Count of checkpoints deactivated included in maintenance summary

8. **Active Checkpoint Safety Check**:
   - [ ] Before VACUUM, verify no active checkpoint (IsActive = true) references timestamps within VACUUM retention window
   - [ ] If conflict detected:
     - Log WARNING to SyncAuditLog
     - Skip VACUUM for that table
     - Alert admin that checkpoint may become unrestorable
     - Recommendation: Increase VACUUM retention or deactivate old checkpoint

9. **Error Handling and Reporting**:
   - [ ] Individual table failures logged to `SyncAuditLog` with specific error message
   - [ ] Pipeline continues processing remaining tables after failure
   - [ ] Pipeline summary report generated at end including:
     - Tables processed (VACUUM + OPTIMIZE counts)
     - Successes and failures (with failure reasons)
     - Total storage reclaimed (aggregate estimate)
     - Total execution duration
     - Checkpoints deactivated count
   - [ ] If more than 3 tables fail VACUUM/OPTIMIZE, send alert email to data admin team
   - [ ] If checkpoint deactivation fails, send alert (critical for retention policy enforcement)

10. **Safety Features**:
    - [ ] Dry-run mode supported (pipeline parameter: DryRun = true)
    - [ ] In dry-run mode: logs operations but doesn't execute VACUUM/OPTIMIZE/checkpoint deactivation
    - [ ] Warning logged if attempting to VACUUM with retention < 7 days (safety threshold)
    - [ ] Warning logged if VACUUM would affect checkpoints still needed for recovery
    - [ ] Configurable safety threshold: minimum VACUUM retention = 7 days (cannot set lower)

11. **Performance Monitoring**:
    - [ ] Maintenance operation metrics logged: duration per table, files removed, storage savings estimate
    - [ ] Historical trend analysis: track storage growth over time (before/after VACUUM)
    - [ ] Alert if maintenance duration exceeds expected threshold (e.g., > 2 hours total)
    - [ ] Metrics visualized in CR-004 dashboard (if implemented)

**Definition of Done**:
- [ ] All acceptance criteria met and tested
- [ ] Pipeline executed successfully for 3 consecutive weeks
- [ ] Storage metrics showing expected reduction after VACUUM (measured via Spark metrics or Lakehouse storage)
- [ ] Query performance validated after OPTIMIZE (measure before/after)
- [ ] Checkpoint retention enforcement working (old checkpoints marked inactive)
- [ ] Dry-run mode tested (no actual operations executed)
- [ ] Safety check tested (VACUUM blocked if would break checkpoint recovery)
- [ ] Error handling tested (individual table failures don't stop pipeline)
- [ ] Documentation: Maintenance pipeline runbook
- [ ] Documentation: How to configure per-table retention policies
- [ ] Documentation: How to set Delta Lake table properties (logRetentionDuration, etc.)
- [ ] Emergency procedure documented: How to manually run maintenance if scheduled job fails
- [ ] Code reviewed and merged to main branch

**Dependencies**: 
- Delta Lake table properties must be configured before enabling VACUUM
- Requires understanding of Delta Lake time travel retention requirements
- Should align with CR-001 checkpoint retention policies

**Estimate**: 12h

**Priority**: Medium-High (Important for production readiness, prevents unbounded storage growth)

**Implementation Notes**:
```python
# Example maintenance notebook structure
from pyspark.sql.functions import current_timestamp, col
import datetime

# Get maintenance configuration
config_df = spark.sql("""
    SELECT TableName, SchemaName, VacuumRetentionHours, EnableVacuum, 
           EnableOptimize, OptimizeZOrderColumns
    FROM metadata.PipelineConfig
    WHERE SyncEnabled = true
""")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
dry_run = dbutils.widgets.get("dry_run") == "true"

vacuum_results = []
optimize_results = []

for row in config_df.collect():
    table_name = f"{row.SchemaName}.{row.TableName}"
    
    try:
        # VACUUM operation
        if row.EnableVacuum:
            if dry_run:
                print(f"[DRY RUN] Would VACUUM {table_name} RETAIN {row.VacuumRetentionHours} HOURS")
            else:
                # Safety check: Verify no active checkpoints would be affected
                checkpoint_check = spark.sql(f"""
                    SELECT COUNT(*) as cnt
                    FROM metadata.CheckpointHistory
                    WHERE TableName = '{row.TableName}'
                    AND IsActive = true
                    AND CreatedDate > current_timestamp() - INTERVAL {row.VacuumRetentionHours} HOURS
                """).collect()[0].cnt
                
                if checkpoint_check > 0:
                    print(f"WARNING: Skipping VACUUM for {table_name} - would affect active checkpoints")
                    # Log warning
                else:
                    spark.sql(f"VACUUM {table_name} RETAIN {row.VacuumRetentionHours} HOURS")
                    # Log success
        
        # OPTIMIZE operation
        if row.EnableOptimize:
            if dry_run:
                print(f"[DRY RUN] Would OPTIMIZE {table_name}")
            else:
                if row.OptimizeZOrderColumns:
                    spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({row.OptimizeZOrderColumns})")
                else:
                    spark.sql(f"OPTIMIZE {table_name}")
                # Log success
                
        vacuum_results.append({"table": table_name, "status": "Success"})
        
    except Exception as e:
        result = {"table": table_name, "status": "Failed", "error": str(e)}
        vacuum_results.append(result)
        # Log error to SyncAuditLog

# Checkpoint retention enforcement
if not dry_run:
    spark.sql("""
        UPDATE metadata.CheckpointHistory
        SET IsActive = false
        WHERE RetentionDate < current_date()
        AND CheckpointType NOT IN ('PrePurge', 'Manual')
        AND IsActive = true
    """)
    # Log checkpoint deactivation

# Generate summary report
print(f"VACUUM: {len([r for r in vacuum_results if r['status'] == 'Success'])} successes, {len([r for r in vacuum_results if r['status'] == 'Failed'])} failures")
```

---

## 8. Summary of Revised Assessment

### Overall Feature Completion: ~30% (Corrected)

| Story | Current Status | Gap | Revised Estimate | Priority |
|-------|---------------|-----|------------------|----------|
| CR-001 | 60% (basic checkpoints) | Batch coordination, retention enforcement | 14h | High |
| CR-002 | 0% (not started) | Entire rollback pipeline | 24h | **CRITICAL** |
| CR-003 | 0% (audit infra exists) | Validation reporting logic | 10h | Medium-High |
| CR-004 Phase 1 | 0% (not started) | Email alerts on failures | 2h | HIGH |
| CR-004 Phase 2 | 0% (not started) | Dashboards and health reports | 6h | Medium |
| SM-001 | 0% (not started) | VACUUM/OPTIMIZE maintenance | 12h | Medium-High |
| **Purge Pipeline** | **100% ✅ (implemented)** | **None** | **0h** | **N/A** |

**Total Remaining Effort**: 68 hours (down from original 56h due to Purge pipeline being complete, but adding SM-001 and refining others)

### Key Decisions from Discussion

1. **Checkpoint Metadata Retention**: Keep indefinitely (never delete), only mark `IsActive = false` when expired. Preserves audit history.

2. **Delta Lake Time Travel**: Default 30-day retention, configurable per table. High-value tables should use 365 days for annual purge rollback capability.

3. **VACUUM is Manual**: MS Fabric does NOT auto-vacuum. Must schedule SM-001 maintenance pipeline or storage will grow unbounded.

4. **Multi-Table Coordination**: Add "batch checkpoints" that represent all tables at a synchronized point in time (not just individual per-table checkpoints).

5. **Validation Thresholds**: Start with zero tolerance for internal consistency. Add small tolerance (1-5%, configurable) only for temporal edge cases if needed during testing.

6. **Purge Pipeline Complete**: Fully implemented at 100% - no additional work needed.

7. **Priority Sequencing**:
   1. Complete CR-001 (foundation for recovery)
   2. Implement CR-002 (core rollback capability - highest value)
   3. Build SM-001 (prevent storage issues)
   4. Add CR-004 Phase 1 (email alerts - quick win)
   5. Implement CR-003 (validation reporting)
   6. Add CR-004 Phase 2 (dashboards - polish)

### Critical Recommendations

1. **Configure Delta Lake Retention First**: Before implementing rollback, ensure all tables have appropriate `logRetentionDuration` set (30-365 days based on table importance).

2. **Implement SM-001 Early**: Don't wait for all recovery features - storage management should happen in parallel to prevent cost issues.

3. **CR-002 is Highest Value**: All the checkpoint infrastructure is useless without the ability to actually rollback. This should be top priority.

4. **Test Rollback Thoroughly**: Full end-to-end testing of checkpoint creation → data corruption → rollback → validation is essential before production use.

5. **Document Operational Procedures**: Technical implementation is only half the battle. Create runbooks for:
   - How to execute a rollback (step-by-step)
   - How to respond to different alert types
   - How to troubleshoot failed rollbacks
   - How to configure retention policies for new tables

6. **Training Required**: Data admins need hands-on training with the rollback pipeline before a real emergency occurs.