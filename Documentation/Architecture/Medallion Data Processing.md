# Microsoft Fabric Medallion Architecture: Complete Implementation Guide for Nonprofit Fundraising

The medallion architecture in Microsoft Fabric provides a structured, three-layer approach to transforming raw Dataverse fundraising data into analytics-ready insights—but success depends on making strategic decisions at each layer about data quality, dimensional modeling, and tooling choices that align with your multi-office data mesh goals.

## Document Navigation

**This guide is organized for a two-phase implementation approach:**

- **Phase 1 (Weeks 1-8)**: Minimal viable pipeline for demo and performance testing
  - See: [Phased Implementation Approach](#phased-implementation-approach) 
  - See: [Phase 1 Orchestration Pattern](#orchestration-master-pipeline-pattern)
  - See: [Phase 1 Code Examples](#code-examples-and-patterns)
  - See: [Phase 1 Implementation Checklist](#phase-1-implementation-checklist)

- **Phase 2 (Weeks 9-24)**: Full-featured production implementation
  - See: [Phase 2 Roadmap](#phase-2-full-featured-implementation-production-ready---weeks-9-24)
  - See: [Phase 2 Orchestration Pattern](#orchestration-master-pipeline-pattern)
  - See: [Phase 2 Code Examples](#phase-2-full-featured-pipeline-code-examples)
  - See: [Data Quality & Enrichment Sections](#essential-data-quality-transformations-for-fundraising)

**Architecture Overview Sections** (apply to both phases):
- [Silver Layer Overview](#silver-layer-implementation-building-your-validated-foundation)
- [Gold Layer Overview](#gold-layer-implementation-analytics-ready-dimensional-models)
- [Power BI Integration (Import Mode)](#integration-with-power-bi-semantic-models)
- [Data Mesh Principles](#data-mesh-implementation-for-multi-office-architecture)
- [Fundraising-Specific Considerations](#fundraising-specific-considerations)

## Why this matters for your organization

Your Bronze layer foundation is solid with daily sync pipelines and comprehensive tracking. Now, the **Silver layer will establish your single source of truth** through validation, deduplication, and constituent matching—critical for accurate donor analytics. The **Gold layer will deliver star schema dimensional models** optimized for Power BI reporting across your multi-office structure. Together, these layers transform fragmented Dataverse records into trustworthy fundraising insights while maintaining office-level autonomy through data mesh principles.

## Understanding your architecture foundation

Microsoft recommends creating Bronze and Silver as Lakehouses with the Gold layer as a Data Warehouse when T-SQL capabilities and optimized BI performance are priorities—which perfectly suits fundraising analytics. Each layer should exist in separate workspaces for governance, with data progressing Bronze → Silver → Gold through increasing levels of refinement. Your existing Bronze layer with IsDeleted, IsPurged, DeletedDate, and LastSynced tracking columns provides the foundation; Silver extends this with data quality scores and validation status, while Gold adds dimensional modeling metadata.

# Silver Layer Implementation: Building Your Validated Foundation

## Essential data quality transformations for fundraising

The Silver layer serves as your "validated data" zone where raw Bronze records become trustworthy. **Your primary objective is establishing data quality across five dimensions**: completeness (non-null required fields), accuracy (business rule validation), consistency (cross-table integrity), timeliness (data freshness), and uniqueness (deduplication). For fundraising data specifically, this means parsing and standardizing donor contact information, validating gift amounts and currencies, normalizing campaign identifiers, calculating lifetime value metrics, and separating pledges from payments.

### Constituent matching and deduplication strategies

Deduplication represents your most critical Silver layer challenge—nonprofits lose 10+ hours weekly to manual deduplication, and duplicate donors skew all downstream analytics. **Implement a three-tier matching strategy**: exact matching using composite SHA-256 hash keys of name/email/phone combinations, fuzzy matching with Soundex or Metaphone for phonetic similarities, and Levenshtein distance calculations for typo detection. Use Spark Window functions to rank duplicates by recency (modifiedon timestamp) and retain the most current record. For complex scenarios involving household relationships and partial matches, consider graph-based deduplication using GraphFrames to identify connected components.

A robust implementation looks like this: generate exact_match_key as SHA-256 hash of lowercased firstname, lastname, email, and phone; create soundex columns for phonetic matching; apply Window partitioning by exact_match_key with row_number ordering by modified date descending; filter to row_num = 1 for deduplicated results. Maintain an audit trail of all merges in a deduplication_log table tracking source_id, target_id, merge_reason, merge_date, and merged_by for compliance and troubleshooting.

### Data cleansing patterns

**Address standardization** requires converting to uppercase for postal matching, expanding abbreviations (St → Street), validating ZIP codes against USPS standards, and optionally adding geocoding coordinates. Use external APIs like PostGrid or Geocodio, or implement custom PySpark UDFs. Store cleaned addresses in separate columns (address1_clean, city_clean, state_clean, postal_clean) while preserving originals.

**Phone formatting** should strip all non-numeric characters, validate 10-digit US format, and format consistently as (###) ###-####. **Email validation** applies regex pattern matching against standard email format, converts to lowercase, trims whitespace, and flags valid/invalid status in boolean columns. These seemingly simple transformations are essential—unvalidated contact data causes failed communications, duplicate records, and inaccurate engagement metrics.

### Household grouping for donor relationships

Household modeling is fundamental to fundraising analytics. Create household groups by generating address_composite keys from standardized address components, using groupBy to collect household members, calculating household_size and household_ytd_giving aggregates, and assigning household_id to all members. **This enables accurate gift attribution**: hard credits go to the primary contact while soft credits go to other household members, preventing double-counting in revenue reports while maintaining relationship visibility.

Track primary_contact designation, shared data like household_income and giving_capacity, and relationship types (spouse, partner, parent, child). Generate both formal greetings ("Mr. and Mrs. Smith") and informal greetings ("John and Jane") automatically. Implement seasonal address management for snowbirds and students with temporary/permanent address flags.

### Handling soft deletes and purged records

Your Bronze tracking columns (IsDeleted, IsPurged) require thoughtful Silver layer handling. **Two proven strategies exist**: maintain-with-flag approach where you create is_active boolean columns calculated as NOT (IsDeleted OR IsPurged), keeping all records in Silver with active-only views for consumption; or archive approach where active records go to silver.donors while deleted records append to silver.donors_archive for historical reference.

For optimal performance, **enable Delta Lake deletion vectors** with ALTER TABLE SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true'). This delivers 10-100x performance improvements by using merge-on-read strategy with bitmap tracking instead of rewriting entire Parquet files. Run REORG TABLE APPLY (PURGE) periodically to compact, and VACUUM with appropriate retention (168 hours for 7-day retention) to reclaim storage.

## Change data capture and slowly changing dimensions

### CDC implementation patterns

Microsoft Fabric offers two CDC approaches for Dataverse. **Dataverse Link to Fabric (strongly recommended)** provides zero-copy architecture with 2-3 minute latency real-time sync, automatic schema evolution, and native Delta Lake format. Enable via Power Apps → Tables → Analyze → Link to Microsoft Fabric, selecting your target workspace. This eliminates custom CDC code and ensures your Silver layer stays current with source changes.

Alternatively, implement watermark-based CDC by maintaining a watermark table tracking entity_name, layer, last_watermark timestamp, and updated_at. Your incremental load queries filter bronze source WHERE modified_timestamp > last_watermark. Use Delta Lake time travel with startingVersion option to read only new data since last processing version. Process inserts, updates, and deletes appropriately—soft deletes update is_deleted flags rather than physically removing records.

### SCD Type 2 for donor history

Tracking donor attribute changes over time is essential for analyzing giving patterns. **Implement SCD Type 2 for key donor dimensions**: generate row_hash from SCD-tracked columns (name, address, donor_tier, wealth_rating), identify changed records by comparing source and target row_hash where is_current = true, close old versions by updating is_current = false and setting end_date = current_timestamp, and insert new versions with start_date = current_timestamp and is_current = true.

This preserves complete history—when analyzing campaigns from 2022, you see donor addresses and segments as they existed in 2022, not current values. Critical for understanding donor migration patterns, lifetime value calculations, and geographic giving trends over time.

## Data enrichment and business rules

### Integrating external data sources

**Silver layer enrichment elevates data value significantly**. For wealth screening data, join external wealth scores by contactid and create major_gift_propensity flags based on wealth_rating and lifetime_giving thresholds. This identifies cultivation prospects for major gift officers.

For email marketing platforms (Mailchimp, Constant Contact, Campaign Monitor), aggregate engagement metrics—emails sent, opens, clicks, engagement scores—by email address, then calculate email_engagement_tier classifications (Highly Engaged, Moderately Engaged, Disengaged). Exclude highly engaged sustainers from additional solicitations to prevent donor fatigue.

For event platforms (Eventbrite, Cvent), summarize events_attended, total_event_spend, and most_recent_event by attendee, then identify VIP attendees based on frequency and spending. Link event engagement to giving patterns—event attendees often become donors, and this enrichment enables targeted post-event cultivation.

### RFM scoring for segmentation

**RFM analysis (Recency, Frequency, Monetary) provides powerful donor segmentation**. Calculate recency_days as days since last gift, then score as quintiles (1-5) using ntile window functions—paradoxically, high recency_days gets low recency_score since recent is better. Score frequency as donation_count quintiles and monetary as lifetime_total quintiles. Sum or concatenate these scores into rfm_composite (e.g., "555" = Champions).

Segment donors accordingly: 13-15 = Champions (recent, frequent, high-value—VIP treatment), 10-12 = Loyal (solid supporters—stewardship focus), 7-9 = Potential (emerging—cultivation opportunities), 4-6 = At Risk (declining engagement—reactivation needed), 1-3 = Lost (minimal engagement—low priority). This segmentation drives personalized communications and optimizes fundraising ROI.

### Business rules for fundraising complexities

**Pledge vs payment tracking** requires joining pledges to aggregated payments by pledge_id, calculating balance_remaining as pledge_amount minus total_paid (coalesced to 0), and assigning pledge_status (Paid in Full, Overdue based on expected_date, or Outstanding). Dashboard metrics show outstanding_balance, overdue_count, and fulfillment_rate for development officers.

**Recurring gift identification** analyzes payment patterns—donors with payments in 6+ months qualify as recurring. Calculate next_expected_gift_date as last_payment_date plus recurring_interval (typically monthly). Flag failed payments immediately for payment method updates, since 85-90% retention for monthly donors represents your most valuable segment.

**Soft credit attribution** unions hard credits (transaction donor gets hard_credit_amount) with soft credits (related individuals get soft_credit_amount with credit_type = "Soft Credit"). Summarize by contactid with pivot by credit_type to show both columns. This prevents revenue double-counting while maintaining relationship visibility—critical for household giving and corporate matching scenarios.

## Microsoft Fabric tooling decision: Notebooks vs Dataflows Gen2

This decision significantly impacts performance, cost, maintainability, and team productivity. The research reveals **Spark Notebooks are 115% cheaper** than Dataflow Gen2 for equivalent compute workloads and offer substantially more power.

### Use Spark Notebooks (PySpark/SparkSQL) when:

You have large datasets exceeding 10 million rows; need complex transformations like graph-based deduplication or fuzzy matching algorithms; require custom business logic and error handling; have performance-critical workloads; and your team possesses Python or Scala expertise. **Notebooks deliver the most powerful transformations** with full Spark capabilities, code reusability through modular functions and helper notebooks, version control via Git integration, comprehensive testing with Great Expectations framework, and flexible error handling and logging.

For Silver layer specifically, notebooks excel because of the deduplication complexity, large transaction volumes in fundraising databases, need for custom matching algorithms, and importance of performance optimization at scale. Create reusable functions like cleanse_donor_data(), standardize_addresses(), validate_emails(), and deduplicate_records() that can be called consistently across entities.

### Use Dataflow Gen2 when:

You have small-to-medium datasets under 10 million rows; need simple transformations (filters, joins, basic aggregations); business users will maintain the logic; require 170+ built-in connectors for diverse sources; or want fast prototyping. **Dataflows shine for citizen developers** with visual Power Query interface familiar to Excel and Power BI users, built-in SCD Type 1 and Type 2 support, column profiling for data quality insights, and fast copy optimization for simple ingestion.

However, Dataflows struggle with performance at scale—slower for heavy transformations, higher costs, limited code reuse, and scalability issues beyond 50 million rows. Reserve Dataflows for smaller entities or when non-technical users need ownership.

### Orchestration with Data Pipelines

**Neither Notebooks nor Dataflows orchestrate themselves—that's where Data Pipelines excel**. Use Pipelines for workflow management with sequential and parallel execution paths, error handling with on-fail routes, robust scheduling controls, calling activities (Notebooks, Dataflows, stored procedures), and best-in-class monitoring. Pipelines don't transform data natively but coordinate transformation tools expertly.

### Recommended Silver layer approach

**Implement a hybrid strategy**: use Notebooks for core donor, donation, and campaign transformations requiring deduplication and complex matching; use Dataflows Gen2 for smaller lookup tables, external file ingestion with multiple connectors, or simple dimensional tables where business users need maintenance access; orchestrate everything with Data Pipelines providing scheduling, dependency management, error handling, and monitoring. This balances power, cost, usability, and maintainability.

## Performance optimization techniques

For Spark Notebooks in Silver layer, **configure strategically**: disable V-Order for write-heavy Silver workloads (spark.sql.parquet.vorder.enabled = false) since Silver is primarily ELT; enable or fine-tune optimize write (spark.databricks.delta.optimizeWrite.enabled with binSize = 157m) balancing write throughput with read performance. Use starter pools (5-10 second startup) for small jobs, custom pools for large transformations, and AutoScale for variable workloads.

**Partition Silver tables strategically** by frequently filtered columns like transaction_date, office_id, or campaign_id. Run OPTIMIZE with ZORDER BY (state, city) on large tables weekly to colocate related data. VACUUM regularly to remove old versions but maintain 168-hour retention for time travel capabilities. Monitor with Capacity Metrics App to track CU consumption and optimize costs.

For Dataflows Gen2, disable staging for time-sensitive workflows, use Fast Copy when source and destination both support it, limit query folding complexity that can slow processing, and partition data at source when possible to reduce transfer volumes.

## Error handling and data lineage

### Quarantine invalid records pattern

**Validation-based quarantine** splits processing into two paths: valid records meeting all criteria (non-null required fields, regex pattern matches, positive amounts) write to silver tables, while invalid records write to quarantine tables with error_reason description, quarantine_date timestamp, and validation_error_details. This prevents bad data from polluting Silver while enabling data stewards to review and remediate issues systematically.

Implement try-catch blocks logging success with rows_processed count and timestamp to etl_logs table, or failures with error_message and stack trace. Configure Pipeline alerting for failed runs, quality scores below thresholds (e.g., 85%), or anomalous row count variances exceeding 20%.

### Lineage tracking approaches

**Fabric provides native lineage visualization** showing relationships between Bronze, Silver, and Gold items automatically in workspace lineage view. For enhanced lineage, integrate with **Microsoft Purview** (recommended for enterprises) by registering and scanning your Fabric tenant—this extracts metadata and lineage automatically, provides sub-item level metadata for lakehouse tables, and centralizes catalog across Azure and Fabric.

Add custom lineage metadata columns: source_table, source_file from input_file_name(), processing_timestamp, processing_notebook identifying transformation code, fabric_workspace, and batch_id linking back to Bronze. Create data_lineage tracking table documenting source_layer, source_entity, target_layer, target_entity, transformation_type, and transformation_logic for comprehensive audit trails.

# Gold Layer Implementation: Analytics-Ready Dimensional Models

## Optimal dimensional modeling for fundraising analytics

The Gold layer transforms validated Silver data into dimensional models optimized for business intelligence. **Star schema is Microsoft's recommended approach** for Fabric Data Warehouse—it delivers high performance relational queries with fewer joins, higher likelihood of useful indexes, and low maintenance as designs evolve.

### Essential fact tables

**FactDonations** represents your core transaction fact at donation line item grain. Key measures include donation_amount, payment_amount, pledge_amount, soft_credit_amount, and hard_credit_amount. Dimension keys link to DimDonor, DimCampaign, DimDate (transaction date), DimPaymentMethod, DimDesignation (fund allocation), DimOffice, and DimSolicitor (fundraiser). Additive measures enable sum, avg, min, max aggregations supporting virtually all fundraising reports.

**FactCampaignPerformance** provides campaign-level daily snapshots. Measures include daily_donations, daily_donor_count, daily_response_rate, cumulative_to_date totals, goal_attainment_pct, and cost_per_dollar_raised. Dimension keys link to DimCampaign, DimDate, DimOffice, and DimChannel (email, direct mail, phone, event). This fact enables real-time campaign dashboards and ROI analysis.

**FactDonorActivity** tracks donor lifecycle events beyond transactions. Captures activities like solicitations_sent, emails_opened, events_attended, volunteer_hours, and advocacy_actions. Links to DimDonor, DimDate, DimActivityType, and DimChannel. Essential for engagement scoring and multi-touch attribution beyond just gifts.

### Critical dimension tables

**DimDonor** represents your primary constituent dimension with **SCD Type 2 implementation** tracking changes over time. Include donor_key (surrogate key), donor_id (business key from Dataverse contactid), donor_name, donor_type (Individual, Organization, Foundation), contact attributes (email, phone, address components), segmentation (donor_segment, rfm_score, engagement_tier, wealth_rating), giving summary (lifetime_total, first_gift_date, last_gift_date, largest_gift_amount, donation_count), and SCD tracking (start_date, end_date, is_current, row_hash).

This SCD Type 2 structure is essential—when analyzing a 2022 campaign, you see donor segments as they existed in 2022, not current classifications. Preserves accurate historical analysis of donor migration patterns, lifetime value trends, and segment performance over time.

**DimCampaign** provides hierarchical campaign structure. Include campaign_key, campaign_id, campaign_name, campaign_type (Annual Fund, Major Gifts, Planned Giving, Special Events), campaign_category, parent_campaign (for hierarchy), fiscal_year, campaign_status, goal_amount, start_date, end_date, and cost_budget. Enable drill-down from program level through campaigns to specific appeals.

**DimDate** is mandatory for time-based analysis. Generate comprehensive date dimension with date_key, full_date, day/month/quarter/year attributes, fiscal_year and fiscal_quarter aligned to your organization's fiscal calendar, day_of_week, is_weekend, is_holiday flags, and giving_season classifications (Year-End, GivingTuesday, Spring Appeal). This enables time intelligence calculations in Power BI and seasonal pattern analysis.

**DimOffice** supports multi-office data mesh architecture. Include office_key, office_id, office_name, office_region, office_type, office_manager, and office_active_status. Combined with row-level security in Power BI, this dimension enables both office-specific views and enterprise rollup reporting while maintaining data isolation.

### Pre-calculated metrics and aggregations

**Gold layer aggregations dramatically improve report performance**. Create AggDonorSummary with donor lifetime value, retention status, LYBUNT/SYBUNT flags, giving trends (increasing, stable, declining), and average gift calculations. Create AggCampaignMetrics with campaign totals, response rates, cost per dollar raised, and goal attainment percentages. Create AggMonthlyGiving with monthly recurring revenue (MRR), sustainer counts, and retention cohorts.

These aggregated tables serve two purposes: significantly faster dashboard load times since Power BI queries hit pre-calculated sums rather than scanning millions of transactions; and encapsulation of complex business logic calculated once in T-SQL rather than repeated in every Power BI measure.

## Data Warehouse vs Lakehouse for Gold layer

This architectural decision impacts performance, functionality, and user experience significantly. **Microsoft's guidance suggests creating Bronze and Silver as Lakehouses with Gold as Data Warehouse** when you need T-SQL DDL/DML capabilities, multi-table transaction support, advanced security features, or third-party reporting tool compatibility requiring TDS/SQL endpoints.

### Choose Data Warehouse for Gold when:

Your team prefers T-SQL for transformations using stored procedures, views, and functions; you need dynamic data masking for sensitive donor data (SSN, wealth ratings); you require granular security with object-level, column-level, and row-level controls; users need to query and modify data post-modeling with DDL/DML support; and third-party reporting tools connect via SQL Server-compatible endpoints.

**Data Warehouse excels for BI-optimized scenarios**. Create star schema tables using CREATE TABLE with explicit primary/foreign keys, use stored procedures for ETL logic orchestrated by Pipelines, implement column-level security and dynamic data masking, and enable semantic model Direct Lake mode for real-time Power BI reporting. The warehouse provides audit logs, granular permissions, and transaction guarantees essential for regulated fundraising environments.

### Choose Lakehouse for Gold when:

Your team's primary skillset is Spark (PySpark/SparkSQL); you don't need advanced Data Warehouse capabilities beyond basic querying; you prefer notebook-based transformations; and you're optimizing for data science and ML workloads alongside BI. Lakehouses offer SQL analytics endpoint (read-only) for T-SQL querying, support unstructured data if needed, integrate seamlessly with Spark notebooks, and cost less for equivalent workloads.

### Hybrid approach (recommended for most scenarios)

**Implement Bronze and Silver as Lakehouses, Gold as Data Warehouse**. This provides optimal cost-performance balance—Lakehouses for high-volume data engineering with Spark notebooks at lower compute costs, transitioning to Data Warehouse for final dimensional model with full T-SQL capabilities and advanced security. Development teams use their Spark expertise for complex Silver transformations, while business analysts and SQL developers build Gold dimensional models with familiar T-SQL. Power BI users get optimal performance via Direct Lake from Warehouse with enhanced security and governance.

## Integration with Power BI semantic models

The Gold layer feeds Power BI through two primary modes. **Import mode (recommended for your implementation)** copies Gold data into Power BI semantic model, providing maximum flexibility for complex DAX calculations, offline access, complete formula control, and proven stability. This approach requires scheduled refreshes and creates data duplication, but delivers the most reliable performance and broadest feature compatibility.

**Direct Lake mode** reads directly from Delta tables in Warehouse or Lakehouse without import or DirectQuery compromises, delivering real-time data with import-like performance. No data movement or duplication occurs—Power BI semantic model points to Gold tables via lakehouse/warehouse connection. Consider Direct Lake for future optimization once your core architecture stabilizes.

Configure **incremental refresh** for large fact tables by defining refresh window parameters (e.g., last 7 days incremental, full refresh older partitions quarterly), using date range parameters in Power Query, and archiving historical partitions. This balances freshness with refresh performance—critical when FactDonations contains millions of rows.

Implement **row-level security** in Gold layer filtering by office_id matching user's assigned office(s). This enforces data isolation for multi-office architecture—Dallas office users see only Dallas data, while enterprise users see consolidated views. Define security roles in Warehouse using CREATE SECURITY POLICY or in Power BI semantic model with DAX filters.

# End-to-End Architecture and Orchestration

## Integrating with your existing Bronze layer

Your Daily Sync Pipeline continues unchanged as foundation. **Extend metadata architecture across layers** by inheriting Bronze tracking columns (LoadDate, ProcessID, SourceFile, IsDeleted, IsPurged, DeletedDate, PurgedDate, LastSynced) into Silver, then adding Silver-specific metadata (processing_timestamp, data_quality_score, validation_status, silver_batch_id), and finally Gold metadata (model_version, refresh_timestamp, fact_grain_description, business_key).

Create checkpoint tracking with dedicated metadata tables. Bronze_metadata_log tracks ingestion metrics, Silver_metadata_log captures transformation statistics (records_read, records_written, records_deduplicated, records_failed, processing_duration_sec, data_quality_score), and Gold_metadata_log monitors dimensional model refreshes (records_inserted, records_updated, records_deleted, scd2_changes, semantic_model_refresh_status).

## Orchestration pattern: Layered pipeline approach

**Implement Master_Medallion_Pipeline orchestrating all layers**. Stage 1 executes existing Bronze Daily_Sync_Pipeline. Stage 2 transforms Bronze → Silver processing independent entities in parallel (Notebook activities for Donors, Campaigns, Donations executing simultaneously) with dependencies waiting for Bronze completion. Stage 3 transforms Silver → Gold sequentially respecting dimensional dependencies (dimensions before facts) using stored procedures in Warehouse. Stage 4 handles post-processing including data quality validation, metadata updates, and semantic model refresh triggers.

Use **On Success connectors** for activity dependencies ensuring proper sequencing. Implement **If Condition activities** for conditional logic (e.g., skip full refresh if incremental succeeds). Set **retry=2** on Notebook activities handling transient Spark failures. Configure **timeout policies** preventing runaway jobs.

## Scheduling and refresh strategies

**Daily batch processing (recommended baseline)** runs Bronze at 2:00 AM during off-peak hours capturing previous day's Dataverse changes, Silver at 3:00 AM after Bronze completes applying all validations and enrichments, Gold at 5:00 AM after Silver completes building dimensional models, and semantic model refresh at 6:30 AM ensuring reports are current by 7:00 AM user arrival.

**Incremental refresh pattern (recommended over full refresh)** uses watermark tables tracking last_watermark timestamp per entity and layer, queries filter source data WHERE modified_timestamp > last_watermark, and Delta Lake MERGE operations upsert changes rather than full overwrites. This dramatically reduces processing time and compute costs—refreshing 10,000 changed records instead of scanning 10 million is 1000x more efficient.

**Event-driven overlay for real-time scenarios** uses Fabric Activator (Reflex) to trigger pipelines on OneLake events, detects file arrivals in Bronze lakehouse folders, and auto-triggers Silver pipeline with file parameters. This achieves sub-15-minute latency for critical updates like major gift notifications or campaign performance during GivingTuesday.

**Late-arriving data handling** implements 3-day lookback windows (refresh last 3 days to catch late arrivals), uses MERGE upsert operations updating existing records when late data arrives, or reprocesses specific date partitions when identified. This is common in fundraising—gifts processed Monday may have Friday transaction dates requiring retroactive updates.

## Metadata management and data lineage

**Fabric native lineage** automatically visualizes item relationships in workspace lineage view showing Bronze → Silver → Gold → Semantic Model → Report flows. **Microsoft Purview integration (enterprise recommendation)** requires registering Fabric tenant in Purview, configuring scans for automatic metadata extraction, accessing table-level metadata for lakehouses (preview), and tracking transformations across workspaces in unified catalog.

**Custom lineage metadata** extends tracking with data_lineage table documenting source_layer, source_entity, target_layer, target_entity, transformation_type, and transformation_logic. Add lineage columns to Silver and Gold tables: source_table, source_file via input_file_name(), processing_timestamp, processing_notebook, fabric_workspace, and batch_id. This creates complete audit trails for compliance and troubleshooting.

**Quality metrics and alerting** calculates scores across dimensions (completeness, accuracy, consistency, timeliness, uniqueness), logs metrics to quality_metrics table, implements quality gates failing pipelines when scores drop below thresholds (e.g., 85%), and sends Data Activator alerts to data stewards for degradation.

# Data Mesh Implementation for Multi-Office Architecture

## Applying data mesh principles

Data mesh aligns perfectly with multi-office fundraising operations. **The four core principles** are domain-oriented ownership (each office owns its fundraising data domain, decentralizing from central data team to office teams); data as a product (treat each office's data as product with defined owners, discoverability, quality SLAs, and documentation); self-serve data platform (central team provides Fabric infrastructure while offices use capabilities autonomously); and federated computational governance (distributed governance with central policies embedded computationally in platform).

### Office-level isolation and autonomy architecture

**Create Fabric workspace per office domain**. Organization_NorthRegion contains Workspaces for NorthRegion_Bronze, NorthRegion_Silver, and NorthRegion_Gold. Organization_SouthRegion contains parallel workspaces for SouthRegion domains. Organization_Enterprise contains Enterprise_Gold workspace for consolidated reporting. Each office gains admin rights to their workspaces, separate Fabric capacities for independent scaling, and cost allocation per domain.

**OneLake shortcuts enable zero-copy integration**. Enterprise workspace creates shortcuts referencing office domain Gold tables, virtually aggregating without duplicating data. Offices maintain complete autonomy over their transformations while enterprise reports consume consolidated views. This architectural pattern delivers both isolation and integration.

**Row-level security enforces data boundaries**. Office users access only their office's data through filters on office_id column, enterprise users access aggregated views, and consolidated semantic models apply dynamic security based on user's assigned office(s) from Azure AD groups.

### Domain ownership model

**Central Platform Team** provisions Fabric capacities and workspaces, maintains medallion architecture templates and best practices, provides enablement training and documentation, and manages core governance policies. They enable rather than control—providing infrastructure and standards without dictating implementations.

**Domain Teams (Office-Level)** own data quality with Office Data Stewards, analytics transformations with Office Analytics Leads, report development with Office Business Analysts, and consumption with end users. Each office customizes Bronze-to-Silver transformations for local data sources, implements office-specific business rules, creates office-specific reports and dashboards, and maintains their own refresh schedules and SLAs.

**Responsibility matrix** clearly delineates: Infrastructure (Central owns, Domains use); Data Ingestion (Central provides templates, Domains customize); Data Quality (Central sets standards, Domains implement); Transformations (Central provides framework, Domains own); Governance (Central defines policies, Domains enforce); Reporting (Central provides platform, Domains create).

### Self-serve platform capabilities

**Provide reusable templates** including Bronze ingestion notebook templates with parameterization, Silver transformation patterns for common cleansing operations, Gold dimensional model templates following star schema best practices, and CI/CD Pipeline templates for deployment automation. Offices clone and customize rather than building from scratch.

**Enable multiple development experiences** accommodating varying skill levels: Notebooks for data engineers proficient in PySpark, Dataflow Gen2 for business analysts familiar with Power Query, T-SQL for SQL developers working in Warehouse, and Power BI for report developers. This poly-glot approach maximizes team productivity.

**Implement deployment automation** with Deployment Pipelines managing Dev → Test → Prod promotion, Git integration for version control and change tracking, automated testing frameworks validating transformations, and parameter management abstracting environment-specific configurations. Offices deploy independently without central bottlenecks.

### Federated computational governance

**Central policies (tenant-level)** define sensitivity labels and classifications (Public, Internal, Confidential, Restricted), enforce naming conventions ([Office]_[Layer]_[Entity]), mandate data retention policies (active 10 years, lapsed 7 years), set security baselines, and document compliance requirements (GDPR, PCI DSS).

**Domain policies (workspace-level)** implement office-specific business rules, set data quality thresholds appropriate to use cases, define refresh schedules meeting local needs, and establish SLA definitions. Offices have autonomy within guardrails.

**Computational enforcement** embeds governance in platform—decorators on transformation functions automatically validate naming conventions, check sensitivity labels, and enforce data quality rules. Use **Microsoft Purview Hub** for centralized governance reporting across domains, endorsement and certification workflows, sensitivity label management, and data classification. Use **Fabric Domains** grouping workspaces by business domain with domain-level permissions and discovery in OneLake data hub.

# Fundraising-Specific Considerations

## Microsoft Dataverse for Nonprofits data model

Your Dataverse source follows the **Nonprofit Common Data Model with 90+ entities**. Key entities for Silver/Gold include: Accounts (households, organizations, businesses); Contacts (individual constituents with demographics, communication preferences, salutations); Donations (commitments, designations, transactions with gift types, amounts, statuses, payment methods); Pledges (payment schedules and fulfillment tracking); Campaigns/Appeals (fundraising initiatives with goals and performance); Memberships (categories and levels); and Awards (grant management lifecycle).

**Critical relationships** flow through the model: Accounts contain multiple Contacts (household members), Contacts link to multiple Accounts through relationships, Donations generate hard and soft credits, Pledges spawn payment schedules, tributees link to notifiees for memorial gifts, and party relationship groups represent households. Your Silver transformations must preserve these relationships through proper foreign key handling.

**Standard fields** require special handling: biographical data needs cleansing and standardization, communication preferences inform outreach segmentation, hard/soft credit tracking prevents double-counting, recurring schedules identify sustainers, tribute information enables memorial gift stewardship, corporate matching eligibility drives matching gift programs, and wealth screening scores guide major gift strategies.

## Essential fundraising KPIs and benchmarks

### Donor retention rate (your most critical metric)

**Industry benchmarks show crisis levels**: overall retention 43.6-45.4% and declining, new donor retention catastrophic at 20-30% (70% never give again after first gift), repeat donor retention 60-70%, but monthly donor retention excellent at 85-90%. Your Gold layer must track retention meticulously—calculate as (donors giving last year AND this year / total donors last year) × 100, segment by donor cohort and gift level, identify inflection points in retention curves, and enable reactivation campaigns targeting at-risk segments.

### Donor lifetime value calculations

LTV determines acquisition investment limits and identifies high-value segments for cultivation. **Two calculation approaches**: simple method as Average donation × Donations per year × Average lifespan (typically 10 years), or perpetuity method as Average donation / (1 - retention rate). A donor giving $100 annually with 60% retention has LTV of $100 / (1 - 0.6) = $250. Pre-calculate LTV in Gold layer DimDonor enabling prioritization of cultivation activities and justifying stewardship investments.

### LYBUNT/SYBUNT analysis

**LYBUNT (Last Year But Unfortunately Not This)** and **SYBUNT (Some Year But Unfortunately Not This)** identify lapsed donors for reactivation. Calculate in Gold AggDonorSummary table with flags: lybunt_flag = (gave in [current_year - 1] AND NOT in current_year), sybunt_flag = (gave in any prior year AND NOT in current_year AND NOT LYBUNT). Run quarterly reports, segment by gift level/frequency/recency, trigger personalized reactivation campaigns, and track anniversary dates for follow-up timing.

### RFM segmentation (pre-calculated)

Store RFM scores in Gold DimDonor: recency_score (1-5 quintile of days since last gift), frequency_score (1-5 quintile of gift count), monetary_score (1-5 quintile of lifetime total), and rfm_composite (concatenated or summed). **Create donor_segment** classifications: Champions (555, recent frequent high-value—VIP treatment), Loyal (strong supporters—stewardship focus), Potential (emerging donors—cultivation), At Risk (declining engagement—reactivation), Lost (minimal engagement—low priority). These segments drive targeted communication strategies.

### Campaign ROI and cost per dollar raised

**Benchmark CPDR varies dramatically by channel**: direct mail acquisition $1.00-$1.50 (acceptable to lose money acquiring donors), direct mail renewal $0.20-$0.35, major gifts $0.05-$0.15, online $0.10-$0.25, events $0.30-$0.50, overall program target below $0.25-$0.30. Track in FactCampaignPerformance: campaign_cost, total_raised, cpdr = cost / raised, roi_pct = ((raised - cost) / cost) × 100. Alert when CPDR exceeds channel benchmarks or ROI falls below thresholds.

## Compliance and regulatory requirements

### GDPR for donor privacy

GDPR applies to ALL organizations collecting data from EU residents regardless of nonprofit location—non-negotiable requirement. **Key mandates** include explicit consent (freely given, informed, specific—no pre-checked boxes), transparency (clear privacy policies in plain language), individual rights (access, correction, deletion, portability, restriction, objection), data protection by design (privacy built into systems from start), and breach notification (report within 72 hours). Penalties reach €20M or 4% of global annual revenue.

**Implementation in your architecture**: Track consent with date/time, method, purpose, source, IP address, and privacy policy version in Bronze/Silver consent_log table; maintain suppression lists permanently for unsubscribe/deletion requests; implement data retention by filtering Silver/Gold to donors with valid retention status; provide deletion APIs reading from OneLake and removing/anonymizing records; and ensure vendor compliance through DPAs with all service providers.

### PCI compliance for payment data

**PCI DSS 4.0.1 mandatory March 31, 2025** significantly impacts nonprofits. Most nonprofits qualify as Level 4 (under 20K e-commerce + under 1M total annual transactions) requiring annual SAQ (Self-Assessment Questionnaire). **SAQ-A (most common)** applies when all payment processing is outsourced using hosted forms or secure JavaScript—you never touch card data on your servers.

**Critical requirements**: document all scripts on donation pages, implement change management and detection processes, conduct quarterly security scans, never store full card numbers (beyond last 4 digits), CVV/CVC codes, PINs, or magnetic stripe data. Violations trigger $5K-$500K fines, increased processing fees, potential loss of card acceptance, and liability for breaches ($100K+).

**Architecture implications**: use PCI-certified (not just compliant) payment processors, prefer redirect forms over embedded forms when possible, never store payment data in Dataverse/Bronze/Silver/Gold—only store transaction_id, last_4_digits, payment_method_type, and processor references. Keep Fabric workspaces and capacity access restricted to authorized personnel only.

# Implementation Roadmap and Best Practices

## Phased implementation approach

### Phase 1: Minimal Viable Pipeline (Demo & Performance Testing) - Weeks 1-8

**Objective**: Establish end-to-end data flow Bronze → Silver → Gold with minimal transformations to validate architecture feasibility, test performance at scale, and demonstrate the medallion concept.

**Phase 1A (Weeks 1-4): Foundation & Minimal Silver**
- Establish workspace structure (Bronze_Lakehouse, Silver_Lakehouse, Gold_Warehouse - single workspace initially)
- Configure Dataverse Link to Fabric for zero-copy CDC (or continue existing Daily Sync Pipeline)
- **Minimal Silver Layer**: Simple pass-through with basic metadata enrichment
  - Copy Bronze tables to Silver with minimal changes
  - Add Silver metadata columns: processing_timestamp, silver_batch_id, data_quality_score (default 100)
  - Preserve all Bronze tracking columns (IsDeleted, IsPurged, DeletedDate, PurgedDate, LastSynced)
  - NO deduplication, NO complex validation, NO enrichment
  - Use simple Notebook or Dataflow Gen2 for pass-through transformation
- Test incremental refresh patterns with watermark tables
- Establish basic monitoring (Pipeline run logs)

**Phase 1B (Weeks 5-8): Minimal Gold & End-to-End Testing**
- **Minimal Gold Layer**: Simplified dimensional model
  - Create basic DimDonor (no SCD Type 2 - just current state with donor_id as business key)
  - Create basic FactDonations (donation_id, donor_id, donation_amount, donation_date)
  - Create simple DimDate (date_key, full_date, year, month, day)
  - Create DimOffice (office_key, office_id, office_name)
  - Use simple INSERT/UPDATE logic in T-SQL stored procedures (no complex SCD handling)
- Build simplified Master_Medallion_Pipeline_v1 (see pattern below)
- **Performance Testing**: 
  - Measure end-to-end processing time for full dataset
  - Test with incremental loads (1 day, 1 week, 1 month of changes)
  - Monitor Capacity CU consumption
  - Identify bottlenecks and optimization opportunities
- Create basic Power BI Import mode semantic model connecting to Gold
- **Demo Deliverables**: 
  - Working Bronze → Silver → Gold pipeline
  - Simple donor/donation dashboard in Power BI
  - Performance metrics documentation
  - Feasibility assessment report

**Success Criteria Phase 1**:
- Complete end-to-end pipeline execution without failures
- Processing time benchmarks established (e.g., X minutes for Y million records)
- Power BI reports refresh successfully from Gold layer
- Capacity sizing recommendations documented
- Green light decision for Phase 2 full implementation

**Phase 1 Testing Matrix** - Document these metrics:

| Metric | Measurement | Target/Baseline | Notes |
|--------|-------------|-----------------|-------|
| **Bronze Load** | Time to complete Daily Sync | Baseline | Current performance |
| **Silver Pass-Through** | Time per entity (Donors, Donations, Campaigns) | < 5 min each | Simple SELECT * operation |
| **Gold Dimension Load** | Time for all dimensions | < 10 min | Simple INSERT/UPDATE |
| **Gold Fact Load** | Time for all facts | Baseline | Depends on transaction volume |
| **End-to-End Runtime** | Bronze start → Gold complete | Baseline | Full pipeline time |
| **Incremental Refresh** | Time for 1 day changes | < 15 min | Phase 2 target |
| **Incremental Refresh** | Time for 1 week changes | < 30 min | Phase 2 target |
| **CU Consumption** | Total CUs per pipeline run | Baseline | For capacity sizing |
| **Data Volume - Donors** | Row count in Bronze/Silver/Gold | Baseline | Track consistency |
| **Data Volume - Donations** | Row count in Bronze/Silver/Gold | Baseline | Track consistency |
| **Power BI Refresh** | Import mode refresh time | < 15 min | Depends on data volume |
| **Report Query Performance** | Dashboard load time | < 3 seconds | User experience target |

**Data Quality Assessment** (document for Phase 2 planning):
- [ ] Duplicate donors identified (estimated %)
- [ ] Missing/invalid email addresses (count)
- [ ] Missing/invalid phone numbers (count)  
- [ ] Incomplete addresses (count)
- [ ] Orphaned donations (donations without donor match)
- [ ] Pledge/payment mismatches
- [ ] Data type issues or validation failures
- [ ] Deleted/purged record patterns

**Architecture Decisions for Phase 2**:
- [ ] Recommended Fabric capacity size (F2, F4, F8, F16, etc.)
- [ ] Lakehouse vs Warehouse for Silver layer
- [ ] Notebook vs Dataflow Gen2 preference (based on team skills)
- [ ] Incremental vs full refresh strategy
- [ ] Partitioning strategy for large tables
- [ ] Multi-office workspace separation approach
- [ ] Estimated Phase 2 development timeline

### Phase 1 Implementation Checklist

**Week 1: Environment Setup**
- [ ] Create Fabric workspace: `ORG_Medallion_Demo`
- [ ] Create Bronze Lakehouse: `bronze_lakehouse` (or use existing from Daily Sync)
- [ ] Create Silver Lakehouse: `silver_lakehouse`
- [ ] Create Gold Data Warehouse: `gold_warehouse`
- [ ] Assign appropriate Fabric capacity (F2 minimum for testing)
- [ ] Configure workspace roles and permissions

**Week 2: Silver Pass-Through Layer**
- [ ] Create notebook: `PassThrough_Donors_BronzeToSilver`
  - Read from `bronze_lakehouse.contacts`
  - Add processing_timestamp, silver_batch_id, data_quality_score
  - Write to `silver_lakehouse.contacts`
- [ ] Create notebook: `PassThrough_Donations_BronzeToSilver`
  - Read from `bronze_lakehouse.donations`
  - Apply same metadata pattern
  - Write to `silver_lakehouse.donations`
- [ ] Create notebook: `PassThrough_Campaigns_BronzeToSilver` (if needed)
- [ ] Test each notebook independently
- [ ] Verify Silver tables created with correct schema
- [ ] Document processing time per notebook

**Week 3: Gold Simple Dimensional Model**
- [ ] Create Gold warehouse schema and tables:
  ```sql
  CREATE SCHEMA gold_warehouse;
  
  CREATE TABLE gold_warehouse.DimDonor (
      donor_key BIGINT IDENTITY(1,1) PRIMARY KEY,
      donor_id VARCHAR(50) NOT NULL,
      donor_name VARCHAR(200),
      email VARCHAR(200),
      phone VARCHAR(50),
      city VARCHAR(100),
      state VARCHAR(50),
      postal_code VARCHAR(20),
      created_date DATETIME DEFAULT GETDATE()
  );
  
  CREATE TABLE gold_warehouse.DimDate (
      date_key INT PRIMARY KEY,
      full_date DATE,
      year INT,
      month INT,
      day INT,
      quarter INT,
      fiscal_year INT
  );
  
  CREATE TABLE gold_warehouse.DimOffice (
      office_key INT IDENTITY(1,1) PRIMARY KEY,
      office_id VARCHAR(50),
      office_name VARCHAR(200)
  );
  
  CREATE TABLE gold_warehouse.FactDonations (
      fact_key BIGINT IDENTITY(1,1) PRIMARY KEY,
      donation_id VARCHAR(50) NOT NULL,
      donor_id VARCHAR(50),
      office_id VARCHAR(50),
      donation_amount DECIMAL(18,2),
      donation_date DATE,
      created_date DATETIME DEFAULT GETDATE()
  );
  ```
- [ ] Create stored procedure: `sp_LoadDimDonor_Simple`
- [ ] Create stored procedure: `sp_LoadDimDate`
- [ ] Create stored procedure: `sp_LoadDimOffice`
- [ ] Create stored procedure: `sp_LoadFactDonations_Simple`
- [ ] Test each stored procedure independently
- [ ] Verify Gold tables populated correctly

**Week 4: Pipeline Orchestration**
- [ ] Create Data Pipeline: `Master_Medallion_Pipeline_v1`
- [ ] Add Execute Pipeline activity → existing `Daily_Sync_Pipeline`
- [ ] Add 3 parallel Notebook activities for Silver pass-through
- [ ] Add sequential Stored Procedure activities for Gold loads
- [ ] Configure dependencies and error handling
- [ ] Test pipeline end-to-end with sample data
- [ ] Document any failures and resolution steps

**Week 5-6: Performance Testing**
- [ ] Run full historical load (all data)
  - Document: Start time, end time, total duration
  - Document: CU consumption from Capacity Metrics
  - Document: Row counts at each layer
- [ ] Run incremental load (1 day of changes)
  - Modify Bronze to simulate incremental
  - Measure processing time difference
- [ ] Run incremental load (1 week of changes)
- [ ] Test with different data volumes
- [ ] Identify bottlenecks (which activities take longest?)
- [ ] Compare Bronze → Silver → Gold data consistency

**Week 7: Power BI Integration**
- [ ] Create Power BI Desktop file
- [ ] Connect to `gold_warehouse` using Import mode
- [ ] Import tables: DimDonor, DimDate, DimOffice, FactDonations
- [ ] Create relationships (star schema)
- [ ] Build simple dashboard:
  - Total donations by month
  - Total donations by office
  - Donor count trend
  - Top 10 donors by lifetime giving
- [ ] Test refresh from Power BI Desktop
- [ ] Publish to Power BI Service
- [ ] Configure scheduled refresh (manual for Phase 1)
- [ ] Document report performance metrics

**Week 8: Documentation & Go/No-Go Decision**
- [ ] Complete Phase 1 Testing Matrix (all metrics documented)
- [ ] Complete Data Quality Assessment
- [ ] Complete Architecture Decisions for Phase 2
- [ ] Write executive summary:
  - What worked well
  - What issues were discovered
  - Estimated Phase 2 effort and timeline
  - Capacity and cost projections
  - Recommendation: Proceed with Phase 2 or revisit approach
- [ ] Present findings to stakeholders
- [ ] Get approval to proceed with Phase 2

---

### Phase 2: Full-Featured Implementation (Production-Ready) - Weeks 9-24

**Objective**: Implement comprehensive data quality, deduplication, enrichment, dimensional modeling, and multi-office data mesh architecture.

**Phase 2A (Weeks 9-12): Silver Core Transformations**
- Replace pass-through Silver with comprehensive transformations
- Build Notebook-based pipelines for:
  - **Donors**: Three-tier deduplication (exact match, fuzzy, Levenshtein), household grouping, address standardization, phone/email validation
  - **Donations**: Amount validation, currency normalization, pledge vs payment logic, soft credit attribution
  - **Campaigns**: Campaign hierarchy normalization, performance metrics calculation
- Implement data quality framework with validation rules and quarantine tables
- Add comprehensive error handling and logging
- Test parallel processing across entities

**Phase 2B (Weeks 13-16): Silver Enrichment & Advanced Features**
- Integrate external data sources:
  - Wealth screening data for major gift propensity
  - Email marketing platforms for engagement metrics
  - Event management systems for attendance tracking
- Implement RFM scoring algorithms
- Build calculated fields: LTV, retention status, donor segmentation
- Add data quality monitoring and alerting

**Phase 2C (Weeks 17-20): Gold Advanced Dimensional Model**
- Replace simple Gold with comprehensive star schema:
  - **DimDonor with SCD Type 2**: Track historical donor attribute changes
  - **DimCampaign**: Hierarchical campaign structure with parent-child relationships
  - **FactDonations**: Comprehensive fact with all measure types
  - **FactCampaignPerformance**: Daily campaign snapshots
  - **FactDonorActivity**: Engagement tracking beyond transactions
  - **Pre-aggregated tables**: AggDonorSummary, AggCampaignMetrics, AggMonthlyGiving
- Implement advanced security: row-level security, dynamic data masking, column-level security
- Build complex stored procedures for SCD Type 2 merges
- Create comprehensive semantic models for Power BI Import mode

**Phase 2D (Weeks 21-24): Data Mesh & Multi-Office Expansion**
- Separate workspaces by office (NorthRegion_Bronze/Silver/Gold, SouthRegion_Bronze/Silver/Gold)
- Implement OneLake shortcuts for enterprise aggregation
- Configure office-level row-level security
- Create domain-specific governance policies
- Provide templates and training to office teams
- Enable self-serve analytics capabilities

**Success Criteria Phase 2**:
- Data quality scores >85% across all entities
- Deduplication reducing donor records by 5-15%
- Complete dimensional model supporting all reporting requirements
- Multi-office architecture with proper isolation and aggregation
- Production-ready monitoring, alerting, and error handling

## Critical success factors and recommendations

### Phase 1: Demo & Performance Testing Focus

**Start simple and measure everything**. Phase 1 is about proving the architecture works and establishing performance baselines—not about transformation quality. Document processing times at each stage, CU consumption patterns, data volumes, and bottlenecks. This data drives Phase 2 optimization decisions.

**Use the simplest possible transformations**. Pass-through notebooks should take 30 seconds each to write. Simple stored procedures without SCD logic take 1-2 hours. Don't optimize prematurely—the goal is a working pipeline you can test at scale, not production-ready code.

**Focus on end-to-end flow validation**. Can data move Bronze → Silver → Gold → Power BI without errors? Does incremental refresh work? Can you reprocess historical data? These answers matter more than data quality in Phase 1.

**Document everything for Phase 2**. Note which transformations will be needed (deduplication, validation, enrichment). Identify data quality issues in Bronze. Flag missing relationships or data. This becomes your Phase 2 requirements backlog.

**Performance benchmarking checklist**:
- [ ] Full load processing time (all historical data)
- [ ] Incremental load processing time (1 day, 1 week, 1 month)
- [ ] CU consumption per pipeline run
- [ ] Data volume by entity (row counts, data size in GB)
- [ ] Bottleneck identification (which stage takes longest?)
- [ ] Capacity sizing recommendation for production
- [ ] Cost estimation per refresh cycle

**Success criteria for Phase 1**:
- Pipeline completes without failures
- Power BI reports refresh successfully
- Performance metrics documented
- Go/no-go decision made for Phase 2

---

### Phase 2: Production-Ready Implementation Focus

**Start with single office/domain** proving the pattern before scaling. Gather feedback from first implementation, iterate on templates and processes, document lessons learned comprehensively, and measure success metrics (processing time, data quality scores, user adoption) before expanding to additional offices.

**Invest heavily in data quality from day one**. Implement Great Expectations framework for automated validation, establish quality gates failing pipelines below thresholds, create quarantine tables for invalid records, monitor quality trends over time, and assign clear data steward ownership. Poor data quality in Silver propagates to Gold making all downstream analytics untrustworthy.

**Prioritize Spark Notebooks for Silver layer** given superior performance (115% cheaper than Dataflow Gen2), flexibility for complex transformations, and scalability to large datasets. Reserve Dataflows for simpler scenarios or citizen developer maintenance. Create reusable function libraries reducing code duplication across entities.

**Implement SCD Type 2 on DimDonor** preserving complete history. When analyzing 2022 campaign results, you need to see donor segments as they existed in 2022 (not current classifications) for accurate historical analysis. This small investment in complexity delivers enormous analytical value.

**Monitor capacity consumption religiously**. Use Fabric Capacity Metrics App tracking CU usage by workspace and item, identifying throttling events, forecasting capacity needs, and optimizing costs. Start with F2 for development/testing but plan production capacity based on measured workloads—likely F8 for small implementations, F16+ for large organizations.

**Version control everything**. Enable Git integration for Notebooks and Pipelines, implement branching strategies (dev, test, prod branches), require pull request reviews for production changes, maintain change documentation, and enable rollback capabilities when issues arise.

**Automate deployment** using Deployment Pipelines for Dev → Test → Prod promotion, parameterizing environment-specific configurations (connection strings, capacity names, workspace IDs), implementing CI/CD with validation gates, and enabling rapid iteration without manual errors.

**Document comprehensively** creating data dictionaries for each layer's tables and columns, pipeline documentation explaining transformation logic, runbooks for common operational issues, governance policies and naming standards, and training materials for office teams. Documentation is your force multiplier enabling self-serve capabilities and reducing support burden.

## Code examples and patterns

### Phase 1: Minimal Viable Pipeline Code Examples

**Silver: Simple pass-through transformation with metadata**

```python
# Notebook: PassThrough_Donors_BronzeToSilver
from pyspark.sql.functions import *
from datetime import datetime

# Parameters
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
table_name = "contacts"

# Read from Bronze (unchanged)
bronze_df = spark.table(f"bronze_lakehouse.{table_name}")

# Add minimal Silver metadata - no transformations
silver_df = bronze_df \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(batch_id)) \
    .withColumn("data_quality_score", lit(100))  # Default score, no validation yet

# Write to Silver (simple overwrite or append)
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"silver_lakehouse.{table_name}")

print(f"Processed {silver_df.count()} records from Bronze to Silver")
```

**Gold: Simple dimensional load (no SCD Type 2)**

```sql
-- Stored Procedure: sp_LoadDimDonor_Simple
-- Simple INSERT/UPDATE pattern, no history tracking
CREATE OR ALTER PROCEDURE gold_warehouse.sp_LoadDimDonor_Simple
AS
BEGIN
    -- Delete all and reload (simple approach for Phase 1)
    TRUNCATE TABLE gold_warehouse.DimDonor;
    
    -- Insert current state from Silver
    INSERT INTO gold_warehouse.DimDonor (
        donor_id,
        donor_name,
        donor_type,
        email,
        phone,
        city,
        state,
        postal_code,
        lifetime_total,
        first_gift_date,
        last_gift_date,
        donation_count,
        created_date
    )
    SELECT 
        contactid AS donor_id,
        CONCAT(firstname, ' ', lastname) AS donor_name,
        'Individual' AS donor_type,  -- Simplified
        emailaddress1 AS email,
        telephone1 AS phone,
        address1_city AS city,
        address1_stateorprovince AS state,
        address1_postalcode AS postal_code,
        0 AS lifetime_total,  -- Will calculate in Phase 2
        NULL AS first_gift_date,  -- Will calculate in Phase 2
        NULL AS last_gift_date,  -- Will calculate in Phase 2
        0 AS donation_count,  -- Will calculate in Phase 2
        GETDATE() AS created_date
    FROM silver_lakehouse.contacts
    WHERE ISNULL(isdeleted, 0) = 0 
      AND ISNULL(ispurged, 0) = 0;
      
    PRINT 'DimDonor loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
END;
```

```sql
-- Stored Procedure: sp_LoadFactDonations_Simple
-- Simple append-only fact load
CREATE OR ALTER PROCEDURE gold_warehouse.sp_LoadFactDonations_Simple
AS
BEGIN
    -- Simple INSERT of new donations (no updates, no deduplication)
    INSERT INTO gold_warehouse.FactDonations (
        donation_id,
        donor_id,
        donation_amount,
        donation_date,
        office_id,
        created_date
    )
    SELECT 
        d.donationid AS donation_id,
        d.contactid AS donor_id,
        d.amount AS donation_amount,
        d.donationdate AS donation_date,
        d.officeid AS office_id,
        GETDATE() AS created_date
    FROM silver_lakehouse.donations d
    LEFT JOIN gold_warehouse.FactDonations f 
        ON d.donationid = f.donation_id
    WHERE f.donation_id IS NULL  -- Only insert new records
      AND ISNULL(d.isdeleted, 0) = 0
      AND ISNULL(d.ispurged, 0) = 0;
    
    PRINT 'FactDonations loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
END;
```

**Simplified Pipeline Activity Configuration**

```json
{
  "name": "Master_Medallion_Pipeline_v1",
  "activities": [
    {
      "name": "Execute_Bronze_Pipeline",
      "type": "ExecutePipeline",
      "description": "Run existing Daily Sync Pipeline",
      "pipelineName": "Daily_Sync_Pipeline"
    },
    {
      "name": "Silver_PassThrough_Donors",
      "type": "SynapseNotebook",
      "dependsOn": ["Execute_Bronze_Pipeline"],
      "notebook": "PassThrough_Donors_BronzeToSilver",
      "parameters": {
        "batch_id": "@pipeline().RunId",
        "table_name": "contacts"
      }
    },
    {
      "name": "Silver_PassThrough_Donations", 
      "type": "SynapseNotebook",
      "dependsOn": ["Execute_Bronze_Pipeline"],
      "notebook": "PassThrough_Donations_BronzeToSilver"
    },
    {
      "name": "Gold_Load_DimDonor",
      "type": "SqlServerStoredProcedure",
      "dependsOn": ["Silver_PassThrough_Donors"],
      "storedProcedureName": "sp_LoadDimDonor_Simple"
    },
    {
      "name": "Gold_Load_FactDonations",
      "type": "SqlServerStoredProcedure", 
      "dependsOn": ["Gold_Load_DimDonor", "Silver_PassThrough_Donations"],
      "storedProcedureName": "sp_LoadFactDonations_Simple"
    }
  ]
}
```

---

### Phase 2: Full-Featured Pipeline Code Examples

**Silver: Donor deduplication with PySpark**

```python
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Read Bronze donors
bronze_df = spark.table("bronze_lakehouse.contacts")

# Generate exact match key
bronze_df = bronze_df.withColumn("exact_match_key",
    sha2(concat_ws("|",
        lower(trim(col("firstname"))),
        lower(trim(col("lastname"))),
        lower(trim(col("emailaddress1"))),
        regexp_replace(col("telephone1"), "[^0-9]", "")
    ), 256)
)

# Rank duplicates by modified date
window_spec = Window.partitionBy("exact_match_key").orderBy(col("modifiedon").desc())
bronze_df = bronze_df.withColumn("row_num", row_number().over(window_spec))

# Keep most recent record
dedupe_df = bronze_df.filter(col("row_num") == 1).drop("row_num")

# Calculate data quality score
dedupe_df = dedupe_df.withColumn("completeness_score",
    (when(col("firstname").isNotNull(), 20).otherwise(0) +
     when(col("lastname").isNotNull(), 20).otherwise(0) +
     when(col("emailaddress1").isNotNull(), 20).otherwise(0) +
     when(col("telephone1").isNotNull(), 20).otherwise(0) +
     when(col("address1_composite").isNotNull(), 20).otherwise(0))
)

# Add Silver metadata
silver_df = dedupe_df.withColumn("silver_batch_id", lit(batch_id)) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("validation_status", lit("validated")) \
    .withColumn("is_active", ~(col("isdeleted") | col("ispurged")))

# Write to Silver with optimization
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableDeletionVectors", "true") \
    .saveAsTable("silver_lakehouse.donors")
```

### Silver: RFM scoring with SparkSQL

```sql
-- Calculate RFM components
CREATE OR REPLACE TEMP VIEW donor_rfm AS
SELECT
    contactid,
    DATEDIFF(CURRENT_DATE(), MAX(donation_date)) AS recency_days,
    COUNT(*) AS frequency,
    SUM(donation_amount) AS monetary_total
FROM silver_lakehouse.donations
WHERE is_active = TRUE
GROUP BY contactid;

-- Score as quintiles
CREATE OR REPLACE TABLE silver_lakehouse.donors_with_rfm AS
SELECT
    d.*,
    NTILE(5) OVER (ORDER BY r.recency_days DESC) AS recency_score,
    NTILE(5) OVER (ORDER BY r.frequency) AS frequency_score,
    NTILE(5) OVER (ORDER BY r.monetary_total) AS monetary_score,
    CASE
        WHEN (recency_score + frequency_score + monetary_score) >= 13 THEN 'Champions'
        WHEN (recency_score + frequency_score + monetary_score) >= 10 THEN 'Loyal'
        WHEN (recency_score + frequency_score + monetary_score) >= 7 THEN 'Potential'
        WHEN (recency_score + frequency_score + monetary_score) >= 4 THEN 'At Risk'
        ELSE 'Lost'
    END AS donor_segment
FROM silver_lakehouse.donors d
LEFT JOIN donor_rfm r ON d.contactid = r.contactid;
```

### Gold: Dimensional model creation with T-SQL

```sql
-- Create DimDonor with SCD Type 2
CREATE TABLE gold_warehouse.DimDonor (
    donor_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    donor_id VARCHAR(50) NOT NULL, -- Business key from Dataverse
    donor_name VARCHAR(200),
    donor_type VARCHAR(50),
    email VARCHAR(200),
    phone VARCHAR(50),
    address_composite VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    donor_segment VARCHAR(50),
    rfm_score INT,
    engagement_tier VARCHAR(50),
    wealth_rating INT,
    lifetime_total DECIMAL(18,2),
    first_gift_date DATE,
    last_gift_date DATE,
    donation_count INT,
    -- SCD Type 2 columns
    start_date DATETIME NOT NULL,
    end_date DATETIME,
    is_current BIT NOT NULL DEFAULT 1,
    row_hash VARCHAR(64)
);

-- Create FactDonations
CREATE TABLE gold_warehouse.FactDonations (
    donation_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    donation_id VARCHAR(50) NOT NULL,
    donor_key BIGINT FOREIGN KEY REFERENCES DimDonor(donor_key),
    campaign_key BIGINT FOREIGN KEY REFERENCES DimCampaign(campaign_key),
    date_key INT FOREIGN KEY REFERENCES DimDate(date_key),
    payment_method_key INT FOREIGN KEY REFERENCES DimPaymentMethod(payment_method_key),
    office_key INT FOREIGN KEY REFERENCES DimOffice(office_key),
    donation_amount DECIMAL(18,2),
    payment_amount DECIMAL(18,2),
    pledge_amount DECIMAL(18,2),
    hard_credit_amount DECIMAL(18,2),
    soft_credit_amount DECIMAL(18,2),
    is_recurring BIT,
    is_memorial BIT,
    is_matching BIT,
    transaction_date DATE,
    processing_timestamp DATETIME
);

-- SCD Type 2 merge stored procedure
CREATE PROCEDURE gold_warehouse.sp_MergeDimDonor
AS
BEGIN
    -- Identify changed records
    WITH ChangedRecords AS (
        SELECT
            s.*,
            SHA2_256(CONCAT(s.donor_name, s.address_composite, s.donor_segment)) AS new_hash
        FROM silver_lakehouse.donors s
        INNER JOIN gold_warehouse.DimDonor t ON s.donor_id = t.donor_id
        WHERE t.is_current = 1
          AND SHA2_256(CONCAT(s.donor_name, s.address_composite, s.donor_segment)) <> t.row_hash
    )
    
    -- Close old versions
    UPDATE gold_warehouse.DimDonor
    SET is_current = 0,
        end_date = GETDATE()
    WHERE donor_id IN (SELECT donor_id FROM ChangedRecords)
      AND is_current = 1;
    
    -- Insert new versions
    INSERT INTO gold_warehouse.DimDonor (
        donor_id, donor_name, donor_type, email, phone,
        address_composite, city, state, postal_code,
        donor_segment, rfm_score, engagement_tier, wealth_rating,
        lifetime_total, first_gift_date, last_gift_date, donation_count,
        start_date, end_date, is_current, row_hash
    )
    SELECT
        donor_id, donor_name, donor_type, emailaddress1, telephone1,
        address1_composite, city_clean, state_clean, postal_clean,
        donor_segment, rfm_score, engagement_tier, wealth_rating,
        lifetime_total, first_gift_date, last_gift_date, donation_count,
        GETDATE() AS start_date,
        NULL AS end_date,
        1 AS is_current,
        SHA2_256(CONCAT(donor_name, address1_composite, donor_segment)) AS row_hash
    FROM ChangedRecords;
END;
```

### Orchestration: Master Pipeline pattern

**Phase 1: Simplified Pipeline (Minimal Viable Pipeline)**

```
Master_Medallion_Pipeline_v1 (Simplified for Demo/Testing)
├─ Stage 1: Bronze Ingestion (existing Daily_Sync_Pipeline)
│   └─ Wait for completion
│
├─ Stage 2: Silver Pass-Through (parallel execution)
│   ├─ Notebook: PassThrough_Donors_BronzeToSilver
│   │   └─ Simple SELECT *, add processing_timestamp, silver_batch_id
│   ├─ Notebook: PassThrough_Donations_BronzeToSilver
│   │   └─ Simple SELECT *, add processing_timestamp, silver_batch_id
│   └─ Notebook: PassThrough_Campaigns_BronzeToSilver
│       └─ Simple SELECT *, add processing_timestamp, silver_batch_id
│   └─ All complete before proceeding (no quality gates in Phase 1)
│
├─ Stage 3: Gold Simple Load (sequential execution)
│   ├─ StoredProcedure: sp_LoadDimDonor_Simple
│   │   └─ Simple INSERT/UPDATE (no SCD Type 2)
│   ├─ StoredProcedure: sp_LoadDimCampaign_Simple
│   │   └─ Simple INSERT/UPDATE
│   ├─ StoredProcedure: sp_LoadDimDate
│   │   └─ Generate date dimension if not exists
│   ├─ StoredProcedure: sp_LoadDimOffice
│   │   └─ Simple INSERT/UPDATE
│   └─ StoredProcedure: sp_LoadFactDonations_Simple
│       └─ Simple INSERT (append only, no updates)
│
└─ Stage 4: Basic Post-Processing
    ├─ Update watermark tables for incremental refresh
    ├─ Log pipeline execution metrics
    └─ (Optional) Trigger Power BI Import refresh via API
```

**Phase 1 Characteristics**:
- **Total Activities**: ~12 activities (simple, easy to debug)
- **Estimated Runtime**: Baseline measurement (document for comparison)
- **Focus**: Data flow validation, not transformation quality
- **Error Handling**: Basic retry logic only
- **No quality gates**: Pipeline continues even with data issues (log them for review)

---

**Phase 2: Full-Featured Pipeline (Production-Ready)**

```
Master_Medallion_Pipeline_v2 (Full Implementation)
├─ Stage 1: Bronze Ingestion (existing Daily_Sync_Pipeline)
│   └─ Wait for completion
│
├─ Stage 2: Silver Transformation (parallel execution with quality checks)
│   ├─ Notebook: Transform_Donors_BronzeToSilver
│   │   ├─ Deduplication (exact, fuzzy, Levenshtein)
│   │   ├─ Household grouping
│   │   ├─ Address standardization
│   │   ├─ Phone/email validation
│   │   └─ Write valid → silver.donors, invalid → quarantine.donors
│   ├─ Notebook: Transform_Donations_BronzeToSilver
│   │   ├─ Amount validation
│   │   ├─ Pledge vs payment logic
│   │   ├─ Soft credit attribution
│   │   └─ Write valid → silver.donations, invalid → quarantine.donations
│   ├─ Notebook: Transform_Campaigns_BronzeToSilver
│   │   ├─ Campaign hierarchy normalization
│   │   ├─ Performance metrics
│   │   └─ Write valid → silver.campaigns, invalid → quarantine.campaigns
│   ├─ Notebook: Enrich_Donors_ExternalData
│   │   ├─ Wealth screening data integration
│   │   ├─ Email marketing engagement
│   │   ├─ Event attendance
│   │   ├─ RFM scoring
│   │   └─ LTV calculation
│   └─ All complete before proceeding
│
├─ Stage 3: Silver Quality Validation (quality gates)
│   ├─ Notebook: Calculate_Quality_Scores
│   │   └─ Assess completeness, accuracy, consistency, timeliness, uniqueness
│   └─ If Condition: Quality_Score >= 85%
│       ├─ True path → Continue to Gold
│       └─ False path → Send Alert to Data Stewards, Fail Pipeline
│
├─ Stage 4: Gold Dimensions (sequential with SCD logic)
│   ├─ StoredProcedure: sp_MergeDimDonor_SCD2
│   │   ├─ Detect changed records via row_hash
│   │   ├─ Close old versions (is_current = 0, end_date = now)
│   │   └─ Insert new versions (is_current = 1, start_date = now)
│   ├─ StoredProcedure: sp_MergeDimCampaign
│   │   └─ Hierarchical campaign dimension with parent-child
│   ├─ StoredProcedure: sp_MergeDimDate
│   │   └─ Comprehensive date dimension with fiscal calendar
│   ├─ StoredProcedure: sp_MergeDimOffice
│   │   └─ Office dimension for multi-office architecture
│   ├─ StoredProcedure: sp_MergeDimPaymentMethod
│   ├─ StoredProcedure: sp_MergeDimDesignation
│   └─ StoredProcedure: sp_MergeDimSolicitor
│
├─ Stage 5: Gold Facts (after dimensions complete)
│   ├─ StoredProcedure: sp_LoadFactDonations
│   │   └─ Comprehensive fact with all measure types
│   ├─ StoredProcedure: sp_LoadFactCampaignPerformance
│   │   └─ Daily campaign snapshots
│   └─ StoredProcedure: sp_LoadFactDonorActivity
│       └─ Engagement tracking
│
├─ Stage 6: Gold Aggregations (performance optimization)
│   ├─ StoredProcedure: sp_BuildAggDonorSummary
│   │   └─ Pre-calc LTV, retention, LYBUNT/SYBUNT
│   ├─ StoredProcedure: sp_BuildAggCampaignMetrics
│   │   └─ Pre-calc campaign ROI, CPDR
│   └─ StoredProcedure: sp_BuildAggMonthlyGiving
│       └─ Pre-calc MRR, sustainer retention
│
└─ Stage 7: Post-Processing & Monitoring
    ├─ Update metadata logs (silver_metadata_log, gold_metadata_log)
    ├─ Run OPTIMIZE on large Silver tables
    ├─ Run OPTIMIZE on large Gold tables
    ├─ VACUUM with 168-hour retention
    ├─ Update watermark tables for incremental refresh
    ├─ Trigger Power BI Import refresh via API
    ├─ Send success notification with processing metrics
    └─ Data Activator: Monitor for quality degradation, SLA breaches
```

**Phase 2 Characteristics**:
- **Total Activities**: ~35-40 activities (comprehensive)
- **Estimated Runtime**: Optimized through partitioning, parallelization
- **Focus**: Data quality, enrichment, dimensional modeling excellence
- **Error Handling**: Comprehensive try-catch, quarantine, retry logic
- **Quality gates**: Pipeline fails if quality scores below thresholds
- **Monitoring**: Real-time alerting via Data Activator

## Monitoring and alerting configuration

Implement **Data Activator (Reflex) alerts** for operational monitoring:

- Pipeline failure → Send Teams notification to data engineering channel with error details
- Data quality score below 85% → Alert data steward with affected entities and validation failures
- Row count variance exceeding 20% from 7-day average → Flag potential data issue requiring investigation
- Processing duration exceeding 2x average → Performance alert triggering capacity review
- Failed GDPR deletion request → Critical alert to compliance team
- Silver-to-Gold transformation errors → Alert BI team that reports may contain stale data

Create **monitoring dashboard in Power BI** with tiles showing: pipeline run status (success/failure/running) across all layers, data quality score trends by entity and office, row counts and growth rates identifying anomalies, processing duration trends spotting performance degradation, capacity CU consumption tracking costs, and SLA compliance percentage against defined targets.

# Conclusion and Key Takeaways

Successfully implementing Silver and Gold layers in Microsoft Fabric for your fundraising organization requires balancing technical excellence with practical business needs. **This two-phase approach ensures you validate the architecture before investing in complex transformations.**

## Phase 1: Prove the Concept

**Your Phase 1 objective is validation, not perfection**. Build the simplest possible Bronze → Silver → Gold pipeline to test data flow, measure performance at scale, and demonstrate the medallion concept to stakeholders. The minimal viable pipeline takes 4-8 weeks to build and provides critical insights:

- Does the architecture handle your data volumes?
- What are actual processing times vs. requirements?
- What capacity sizing is needed for production?
- Where are the bottlenecks that need optimization?
- What data quality issues exist in source systems?

**Phase 1 success criteria**: End-to-end pipeline executes without failures, performance benchmarks are documented, Power BI reports work with Import mode, and you have confidence to proceed with Phase 2 production implementation.

## Phase 2: Build Production Quality

**Phase 2 transforms your proof-of-concept into production-ready analytics**. Your Silver layer establishes trustworthy data through rigorous validation, deduplication, and enrichment—the foundation for all downstream analytics. Your Gold layer delivers business-ready insights via dimensional models optimized for Power BI consumption and multi-office reporting.

**Critical decisions drive success**: Use Spark Notebooks for Silver transformations to gain performance and cost advantages (115% cheaper than Dataflow Gen2); implement Data Warehouse for Gold to enable T-SQL capabilities and advanced security; apply SCD Type 2 on DimDonor to preserve historical accuracy; orchestrate with layered pipelines respecting dependencies; and embrace data mesh principles for office autonomy within federated governance.

## Fundraising-Specific Excellence

**Fundraising-specific requirements demand attention**: Track donor retention as your north star metric (industry average 43-45% and declining); calculate lifetime value guiding cultivation investments; implement RFM segmentation driving targeted communications; respect GDPR and PCI compliance protecting donor trust; and pre-calculate KPIs like LYBUNT/SYBUNT enabling proactive reactivation campaigns.

**Start focused and scale deliberately**. Phase 1 proves the pattern with minimal complexity (4-8 weeks). Phase 2 implements production quality with comprehensive transformations (16+ weeks). Gather feedback, document lessons learned, measure success metrics, and ensure stakeholder buy-in before expanding to additional offices. The medallion architecture in Fabric, properly implemented with this phased approach, will transform your fragmented fundraising data into a unified analytics platform driving better donor relationships and greater mission impact.

## Quick Reference: Phase 1 vs Phase 2

| Aspect | Phase 1 (Weeks 1-8) | Phase 2 (Weeks 9-24) |
|--------|---------------------|----------------------|
| **Objective** | Validate architecture feasibility | Production-ready implementation |
| **Silver Layer** | Pass-through with metadata | Full deduplication, validation, enrichment |
| **Gold Layer** | Simple tables, no SCD | Star schema with SCD Type 2 |
| **Pipeline** | ~12 activities, basic logic | ~40 activities, comprehensive |
| **Focus** | Data flow, performance testing | Data quality, dimensional modeling |
| **Power BI** | Basic Import mode dashboard | Production semantic models with RLS |
| **Deliverable** | Feasibility report, go/no-go | Production analytics platform |
| **Team Effort** | 1-2 data engineers | Full team (engineers, analysts, stewards) |