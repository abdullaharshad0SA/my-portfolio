# Data Quality Validation — Bidstream QA Framework

## Problem Statement

Production data pipelines process millions of bidstream events daily across 4 distributed Redshift clusters. A critical visibility gap exists: technical pipelines succeed without throwing errors, but frequently output incorrect, stale, or duplicated metrics downstream.

**Risk**: Business leadership makes high-stakes spending decisions based on corrupted or inconsistent reporting metrics.

## Solution Overview

An automated, hourly QA checker that detects bidstream discrepancies before they propagate to downstream consumers. The system:

1. Extracts raw bidstream logs across 4 production Redshift clusters
2. Aggregates core performance metrics (cost, impressions, clicks, conversions)
3. Compares against authoritative aggregated records in TiDB
4. Applies SKAdNetwork (SKAN) deduplication adjustments
5. Calculates absolute and percentage variances for all metrics
6. Flags incidents exceeding configurable discrepancy thresholds (>0.5%)
7. Posts real-time alerts to Slack for rapid incident response
8. Upserts complete audit log to Snowflake for historical tracking and escalation workflows

## Architecture

### Flow Diagram

```
[Step 1] Environment Setup
    ↓
[Step 2] Loop Over Target Date Window (T-1 lookback)
    ↓
[Step 3] Extract & Aggregate Bidstream from 4 Redshift Clusters
    ↓
[Step 4] Fetch Aggregated Metrics from Authoritative TiDB
    ↓
[Step 5] Apply SKAdNetwork Deduplication Adjustments
    ↓
[Step 6] Compute Absolute & Percentage Variances
    ↓
[Step 7] Evaluate Automation Flags (>0.5% Threshold)
    ↓
[Step 8] Post Incidents to Slack + Upsert Audit Log to Snowflake
```

### Key Configuration

**Lookback Window**: T-1 (yesterday's records only)
- Historical note: Previously ran 3-day lookback, reduced to 1 day as infrastructure stabilized

**Discrepancy Threshold**: 0.5% variance
- Roadmap: Reduce to 0.1% as platform transitions from Redshift to Iceberg

**Incident Escalation**: 
- Automated Kestra job processes audit log
- Evaluates severity (P1/P2/P3)
- Routes templated alerts to stakeholders (Data team, Engineering, Leadership)

## Code Structure

```
advertiser_qa.py
├── Slack connection initialization
├── Team alert posting (hourly run notification)
├── Date loop (T-1 lookback)
├── 4-cluster horizontal data extraction
├── TiDB authoritative metric fetch
├── SKAdNetwork deduplication logic
├── Variance calculation (absolute & percentage)
├── Threshold evaluation and flagging
├── Slack incident posting
└── Snowflake audit log upsert
```

## Metrics Validated

| Metric | Source | Validation |
|--------|--------|-----------|
| Cost (in USD) | Redshift | `SUM(cost) / 1000000.0` |
| Impressions | Redshift | `SUM(has_won)` |
| Clicks | Redshift | `SUM(has_click)` |
| Conversions | Redshift | `SUM(has_conv)` |
| Long-tail Conversions | Redshift | `SUM(has_ltconv)` |

## Technical Highlights

### Multi-Cluster Querying
```python
clusters_data = []
for i in range(4):  # Iterate across 4 distinct production shards
    engine = RS_Daily_conn_func(i)
    df = pd.read_sql(query_daily, engine)
    df['Cluster'] = str(i)
    clusters_data.append(df)

combined = pd.concat(clusters_data)
```

### Variance Calculation
```
Absolute Variance = |Redshift_Value - TiDB_Value|
Percentage Variance = (Absolute Variance / TiDB_Value) × 100%

Flag Incident if Percentage Variance > 0.5%
```

### Slack Integration
Real-time alerts with structured status updates posted to team monitoring channels.

## Integration Points

- **Orchestration**: Kestra (scheduled hourly execution)
- **Alerting**: Slack WebClient API
- **Audit**: Snowflake (historical logging)
- **Escalation**: Templated email workflows to stakeholders

## Operational Notes

- **Execution Time**: Typically completes within 5-10 minutes
- **Data Retention**: Audit logs preserved in Snowflake for 90+ days
- **On-Call**: Data team owns incident response during business hours
- **Threshold Tuning**: Escalation paths reviewed quarterly as data quality improves

## Future Roadmap

1. **Threshold Reduction**: Lower variance threshold from 0.5% → 0.1% post-Iceberg migration
2. **Expanded Metrics**: Add additional KPIs (attributed revenue, custom segments)
3. **Predictive Flagging**: Machine learning models to predict future discrepancies
4. **Cross-Product Validation**: Extend QA framework to other data products (audience segments, creative analytics)

