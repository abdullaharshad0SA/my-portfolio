# Analytics Engineering Portfolio

A collection of production data pipelines and analytics solutions demonstrating expertise in data modeling, pipeline orchestration, data quality, and business intelligence infrastructure.

## 🎯 Overview

This portfolio showcases end-to-end analytics engineering solutions built across a high-volume programmatic advertising platform. Each project demonstrates core competencies in data integration, transformation, validation, and operational monitoring—key requirements for analytics engineering roles.

**Platform Context**: Built on a real-time bidstream infrastructure processing millions of daily events across distributed Redshift clusters, with multi-destination delivery patterns (S3, SFTP, BigQuery, Google Drive).

---

## 📊 Projects

### 1. [Data Quality Validation](./data-quality-validation/) — QA Framework
**Problem**: Silent data discrepancies between bidstream logs (Redshift) and aggregated metrics (TiDB) propagate downstream errors.

**Solution**: Automated hourly QA checker that:
- Extracts raw bidstream logs across 4 production Redshift clusters
- Aggregates performance metrics and compares against authoritative TiDB records
- Applies SKAdNetwork deduplication adjustments
- Flags discrepancies exceeding configurable thresholds (0.5%+)
- Posts incidents to Slack and upserts audit logs to Snowflake
- Integrated with Kestra for orchestration and escalation workflows

**Key Skills**: Multi-cluster data extraction, anomaly detection, data reconciliation, Slack API integration, automated alerting

---

### 2. [Identity Resolution Pipeline](./identity-resolution-pipeline/) — Log-Level Stitching & Enrichment
**Problem**: Client requires programmatic log-level advertising data enriched with proprietary identity tokens, but identity mapping involves complex graph resolution and collision handling.

**Solution**: End-to-end PySpark pipeline that:
- Ingests raw bidstream logs from Redshift (timezone normalization, micro-dollar conversion)
- Resolves entity metadata via MySQL lookups (campaign names, advertiser hierarchies, segment enrichment)
- Implements large-scale identity graph flattening (80GB+ datasets) using PySpark window functions
- Handles 1-to-many identity collisions using deterministic random assignment (6% anomalies)
- Achieves ~70% fill rate on proprietary IQVIA synthetic keys (exceeding 50% target)
- Delivers compressed PSV files via secure SFTP, with automated execution logging

**Key Skills**: PySpark optimization, identity resolution, graph flattening, collision handling, complex data enrichment, SFTP delivery automation

---

### 3. [Log-Level Delivery Automation](./log-level-delivery-automation/) — Scalable Template Engine
**Problem**: Custom log-level data extractions are manual, repetitive, and time-consuming. Each client request requires new SQL, timezone handling, and delivery coordination.

**Solution**: Parametrized orchestration template that:
- Eliminates manual SQL writing through standardized extraction patterns
- Accepts runtime parameters from Kestra (client_name, date_range, destination, filters)
- Implements polymorphic delivery adapter (Google Drive, BigQuery, S3, SFTP)
- Uses in-memory buffer streaming to prevent disk exhaustion on large datasets
- Integrates dynamic Slack notifications with user tagging and real-time status updates
- Planned roadmap: Direct Freshservice ticket integration for true zero-touch self-service

**Impact**: Saved 100+ manual analyst hours in Q2 alone. Transitioning to fully autonomous end-to-end engine.

**Key Skills**: Template-driven architecture, parametrization, polymorphic design patterns, in-memory streaming, orchestration integration, infrastructure automation

---

### 4. [Attribution Modeling](./attribution-modeling/) — Multi-Touch Marketing Attribution
**Problem**: Marketing ROI is difficult to quantify in complex B2B sales cycles where prospects interact with multiple touchpoints before conversion.

**Solution**: Automated attribution framework that:
- Ingests raw marketing interaction logs and correlates with CRM pipeline value
- Implements strict input sanitization with anomaly detection (orphan records, data collisions, timeline inversions, duplicates)
- Builds clean chronological fact tables using Pandas windowing functions
- Applies four simultaneous attribution models (First Touch, Last Touch, Even Split, Position-Weighted U-Shaped)
- Normalizes fractional allocations to guarantee 100% revenue distribution accuracy
- Structured with robust error handling and fallback processing

**Key Skills**: Data validation pipelines, statistical modeling, fact table construction, error handling, pandas optimization

---

### 5. [Data Pipeline Monitoring](./data-pipeline-monitoring/) — Health Check & Failure Detection
**Problem**: Automated pipelines succeed technically but output stale, duplicated, or unreconciled data, leading to silent failures in downstream reporting.

**Solution**: Comprehensive monitoring framework using OOP principles and Python dataclasses that executes six multi-vector integrity tests:
- **Missing Values Test**: Detects null indicators and structural gaps
- **Referential Integrity Test**: Validates foreign key relationships and identifies orphan rows
- **Uniqueness Test**: Prevents duplicate primary keys from sync bugs or improper upserts
- **Freshness Test**: Measures synchronization lag against strict thresholds
- **Domain Constraints**: Enforces business logic rules (valid pipeline stages, categories)
- **Financial Reconciliation**: Audits revenue consistency between systems via primary key joins

**Key Skills**: OOP design, data validation frameworks, monitoring systems, business logic enforcement, financial auditing

---

### 6. [Reach & Frequency Analysis](./reach-frequency-analysis/) — Audience Saturation Metrics
**Problem**: Basic impression counts don't reveal audience saturation. Campaign health requires understanding unique reach and exposure frequency distribution to prevent ad fatigue.

**Solution**: Automated pipeline that:
- Processes raw log-level bidstream data with localized timezone normalization
- Enriches logs with operational campaign metadata via database joins
- Computes deduplicated audience reach (unique user counts)
- Calculates average frequency (total impressions / unique users)
- Generates both exact-count buckets (1x, 2x, 3x exposure) and cumulative windows (1+, 5+, 10+, 20+, 50+ impressions)
- Provides weekly slices and total campaign flight summaries for saturation analysis

**Key Skills**: Log-level data processing, aggregation patterns, frequency distribution modeling, multi-granularity reporting

---

### 7. [SQL Models](./sql-models/) — dbt Analytics Models
**Problem**: Campaign reporting requires clean fact and dimension tables with consistent definitions for downstream BI layers.

**Solution**: dbt models demonstrating:
- **Conversion Path Model**: Event-level grain with deduplication, conversion journey rollup, time-to-conversion buckets, and campaign enrichment
- **Campaign Delivery Quality Report**: Campaign-daily grain with delivery aggregation, goal/budget logic, conversion-path reconciliation, and reporting health flags

**Key Skills**: SQL optimization, dbt best practices, fact/dimension modeling, BI layer design

---

## 🛠️ Technology Stack

- **Languages**: Python, SQL, Spark (PySpark)
- **Data Platforms**: Redshift, Snowflake, BigQuery, MySQL, Iceberg
- **Orchestration**: Kestra, dbt
- **Libraries**: Pandas, PySpark, NumPy, SQLAlchemy
- **Integrations**: Slack API, SFTP (Paramiko), Google Drive/BigQuery APIs, AWS S3
- **Concepts**: Data validation, PII handling, identity resolution, pipeline monitoring, multi-touch attribution, data quality

---

## 🔐 Security & Compliance

All pipelines implement strict data governance patterns:
- **PII Masking**: Regional-aware column censoring (geo-restricted fields, cookie consent validation)
- **Device ID Hashing**: SHA256 hashing for sensitive device identifiers
- **Structured Access Controls**: Role-based data filtering based on user geography and consent status

---

## 📈 Key Patterns & Learnings

### Distributed Data Extraction
- **Multi-cluster querying**: Horizontal distribution across 4+ production Redshift shards with automatic shard selection based on advertiser ID
- **Large-scale filtering**: Query optimization to prevent memory exhaustion when processing millions of records

### Identity & Graph Processing
- **1-to-many collision resolution**: Deterministic random assignment for ambiguous mappings (validated with internal Identity teams)
- **Efficient graph flattening**: PySpark window functions for resolving identity hierarchies at scale

### Polymorphic Data Delivery
- **Adapter pattern**: Decoupled extraction from routing logic, supporting multiple destinations dynamically
- **Stream processing**: In-memory buffer patterns to prevent disk exhaustion

### Operational Excellence
- **Automated monitoring**: Health checks for missing values, referential integrity, freshness, and financial reconciliation
- **Real-time alerting**: Structured Slack notifications with dynamic user tagging and status tracking

---

## 📋 Structure
```
.
├── README.md                              # This file
├── ARCHITECTURE.md                        # Technical patterns & shared functions
├── .gitignore
├── data-quality-validation/
├── identity-resolution-pipeline/
├── log-level-delivery-automation/
├── attribution-modeling/
├── data-pipeline-monitoring/
├── reach-frequency-analysis/
└── sql-models/
```

## 📝 Notes

- All data and client identifiers in code examples have been anonymized or generated
- Python scripts are structured for production execution (Linux EC2, containerized environments)
- PII/security fields have been included to demonstrate compliance awareness

