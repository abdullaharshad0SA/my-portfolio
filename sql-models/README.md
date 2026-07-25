# SQL Models — dbt Analytics Layer

## Overview

This directory contains dbt models demonstrating analytics engineering best practices for campaign reporting and measurement infrastructure. These models show how to build clean, testable fact and dimension tables that support downstream BI and decision-making.

**Note**: These models use placeholder upstream table names for demonstration. Adapt to match your specific dbt project environment.

## Models

### 1. `int_marketing_event_conversion_path.sql` — Intermediate Model

**Grain**: One row per deduplicated conversion event

**Purpose**: Enriches raw marketing interaction events with conversion journey context, campaign metadata, and time-to-conversion analysis.

#### Key Transformations

**Deduplication**:
```sql
-- Identify duplicated conversion events
WITH deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY created_at DESC) AS rn
    FROM stg_marketing_events
    WHERE has_conversion = 1
)
SELECT * FROM deduplicated WHERE rn = 1
```

**Conversion Journey Rollup**:
```sql
-- Build touchpoint sequence for each opportunity
SELECT 
    opportunity_id,
    event_sequence,
    ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY event_date) AS touch_order,
    SUM(CASE WHEN has_conversion = 1 THEN 1 ELSE 0 END) 
        OVER (PARTITION BY opportunity_id ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_conversions
FROM events_enriched
```

**Time-to-Conversion Buckets**:
```sql
-- Categorize conversion velocity
SELECT 
    opportunity_id,
    CASE 
        WHEN days_to_conversion <= 7 THEN 'fast'
        WHEN days_to_conversion <= 30 THEN 'standard'
        WHEN days_to_conversion <= 90 THEN 'extended'
        ELSE 'long_tail'
    END AS conversion_velocity,
    days_to_conversion
FROM opportunity_analysis
```

**Campaign Enrichment**:
```sql
-- Join to campaign metadata
SELECT 
    e.*,
    c.campaign_name,
    c.campaign_type,
    c.channel,
    c.start_date,
    c.end_date
FROM events_enriched e
LEFT JOIN dim_campaigns c 
    ON e.campaign_id = c.campaign_id
```

#### Output Schema

| Column | Type | Description |
|--------|------|-----------|
| event_id | STRING | Unique event identifier |
| opportunity_id | STRING | CRM opportunity reference |
| event_date | DATE | When the interaction occurred |
| campaign_id | STRING | Campaign that generated event |
| campaign_name | STRING | Human-readable campaign name |
| channel | STRING | Marketing channel (email, web, event) |
| event_type | STRING | Interaction type (view, click, conversion) |
| event_value | DECIMAL | Revenue impact if conversion |
| days_to_conversion | INT | Elapsed days from event to deal close |
| conversion_velocity | STRING | fast / standard / extended / long_tail |
| cumulative_conversions | INT | Running count of conversions before this event |
| is_duplicate_event | BOOLEAN | Flag for deduped events |
| created_at | TIMESTAMP | Model execution timestamp |

#### Use Cases

- **Multi-touch Attribution**: Join to calculate fractional revenue allocation
- **Conversion Velocity Analysis**: How long between first touch and conversion?
- **Channel Performance**: Which channels generate fastest conversions?
- **Frequency Distribution**: How many events before conversion occurs?

### 2. `rpt_campaign_delivery_quality_daily.sql` — Report Model

**Grain**: One row per campaign per activity date

**Purpose**: Campaign-level operational reporting showing delivery performance, goal attainment, and data quality indicators. Serves as the authoritative source for BI dashboards and executive reporting.

#### Key Transformations

**Daily Aggregation**:
```sql
-- Aggregate performance metrics by campaign and date
SELECT 
    activity_date,
    campaign_id,
    campaign_name,
    SUM(impressions) AS daily_impressions,
    SUM(clicks) AS daily_clicks,
    SUM(conversions) AS daily_conversions,
    SUM(cost_usd) AS daily_spend,
    AVG(ctr) AS daily_ctr,
    AVG(conversion_rate) AS daily_conversion_rate
FROM fct_campaign_daily_delivery
GROUP BY activity_date, campaign_id, campaign_name
```

**Goal/Budget Logic**:
```sql
-- Compare actual delivery against targets
SELECT 
    campaign_id,
    activity_date,
    daily_impressions,
    daily_spend,
    goal_impressions,
    daily_spend / NULLIF(goal_impressions, 0) AS spend_per_impression,
    ROUND(daily_impressions / NULLIF(goal_impressions, 0), 2) AS goal_attainment_pct,
    CASE 
        WHEN daily_spend > (daily_budget * 1.10) THEN 'overrun'
        WHEN daily_spend < (daily_budget * 0.90) THEN 'underspent'
        ELSE 'on_track'
    END AS budget_status
FROM campaign_performance
```

**Conversion-Path Reconciliation**:
```sql
-- Validate consistency between event-level and aggregated conversion counts
WITH event_level_convs AS (
    SELECT 
        campaign_id,
        activity_date,
        COUNT(DISTINCT opportunity_id) AS unique_conversions,
        SUM(conversion_value) AS total_conversion_value
    FROM int_marketing_event_conversion_path
    WHERE has_conversion = 1
    GROUP BY campaign_id, activity_date
),
aggregated_convs AS (
    SELECT 
        campaign_id,
        activity_date,
        conversions,
        conversion_value
    FROM fct_campaign_daily_delivery
)
SELECT 
    e.campaign_id,
    e.activity_date,
    e.unique_conversions,
    a.conversions,
    ABS(e.unique_conversions - a.conversions) AS conversion_discrepancy
FROM event_level_convs e
LEFT JOIN aggregated_convs a 
    ON e.campaign_id = a.campaign_id 
    AND e.activity_date = a.activity_date
WHERE ABS(e.unique_conversions - a.conversions) > 0
```

**Reporting Health Flags**:
```sql
-- Data quality indicators
SELECT 
    campaign_id,
    activity_date,
    CASE 
        WHEN impressions = 0 THEN 'no_delivery'
        WHEN daily_spend IS NULL THEN 'missing_cost_data'
        WHEN conversion_rate > 0.50 THEN 'suspicious_high_rate'
        WHEN last_data_sync > CURRENT_TIMESTAMP - INTERVAL 4 HOUR THEN 'stale_data'
        ELSE 'healthy'
    END AS data_quality_flag,
    last_data_sync,
    row_count_delivered
FROM campaign_performance
```

#### Output Schema

| Column | Type | Description |
|--------|------|-----------|
| activity_date | DATE | Reporting date |
| campaign_id | STRING | Campaign identifier |
| campaign_name | STRING | Campaign name |
| daily_impressions | INT | Impressions delivered |
| daily_clicks | INT | Clicks received |
| daily_conversions | INT | Conversion events |
| daily_spend | DECIMAL | Media spend (USD) |
| daily_ctr | DECIMAL | Click-through rate (%) |
| goal_impressions | INT | Daily impression target |
| spend_per_impression | DECIMAL | Cost per impression |
| goal_attainment_pct | DECIMAL | % of daily impression goal met |
| budget_status | STRING | overrun / underspent / on_track |
| unique_conversions | INT | Deduplicated conversion count |
| conversion_discrepancy | INT | Delta between event-level and aggregated |
| data_quality_flag | STRING | healthy / no_delivery / missing_cost_data / stale_data |
| last_data_sync | TIMESTAMP | When underlying data was refreshed |
| created_at | TIMESTAMP | Model execution timestamp |

#### Use Cases

- **Executive Dashboard**: Daily performance vs. targets
- **Budget Tracking**: Is campaign on track spend-wise?
- **Conversion Reporting**: How many closed opportunities attributed?
- **Data Quality Monitoring**: Are there discrepancies or sync delays?
- **Operational Alerts**: Alert if budget overrun, delivery gaps, or stale data

## Assumed Upstream Models

These staging/fact tables should exist in your dbt project:

### `stg_marketing_events`
Raw marketing interactions with:
- event_id, event_date, campaign_id, opportunity_id
- has_conversion, event_type, event_value
- created_at, updated_at

### `dim_campaigns`
Campaign master data with:
- campaign_id, campaign_name, campaign_type, channel
- start_date, end_date, budget, goal_impressions

### `fct_campaign_daily_delivery`
Daily aggregated performance with:
- campaign_id, activity_date
- impressions, clicks, conversions, cost_usd
- goal_impressions, daily_budget

## Testing Strategy

### Data Freshness Tests
```yaml
tests:
  - dbt_utils.recency:
      datepart: day
      interval: 1
      field_name: created_at
```

### Uniqueness Tests
```yaml
tests:
  - unique:
      column_name: [activity_date, campaign_id]  # Primary key
```

### Referential Integrity Tests
```yaml
tests:
  - relationships:
      to: ref('dim_campaigns')
      field: campaign_id
```

### Custom SQL Tests
```sql
-- Validate conversion reconciliation
SELECT * FROM {{ ref('rpt_campaign_delivery_quality_daily') }}
WHERE ABS(conversion_discrepancy) > 5
HAVING COUNT(*) > 0
```

## dbt Configuration

Add to `dbt_project.yml`:

```yaml
models:
  analytics:
    intermediate:
      int_marketing_event_conversion_path:
        materialized: table
        schema: analytics_intermediate
        indexes:
          - columns: [opportunity_id, activity_date]
        
    marts:
      rpt_campaign_delivery_quality_daily:
        materialized: view
        schema: analytics_marts
        tags: [daily_refresh]
```

## Execution & Refresh

### Daily Refresh (Production)
```bash
dbt run --models +rpt_campaign_delivery_quality_daily
dbt test --models rpt_campaign_delivery_quality_daily
```

### Full Rebuild (Monthly)
```bash
dbt run --full-refresh --models +rpt_campaign_delivery_quality_daily
```

## Performance Considerations

1. **Indexing**: Create indexes on foreign keys (campaign_id, opportunity_id)
2. **Partitioning**: Partition by activity_date for efficient date-range queries
3. **Materialization**: Use `table` for intermediate models, `view` for reports
4. **Incremental Loads**: Implement `dbt incremental` for large fact tables

## Documentation & Lineage

All models include:
- Field-level documentation
- Data dictionary comments
- Owner and SLA information
- dbt `doc` blocks for markdown documentation

Access via `dbt docs generate && dbt docs serve`

## Roadmap

1. **Incremental Materialization**: Switch to incremental for performance
2. **ML Predictions**: Add predicted conversion probability per event
3. **Cohort Analysis**: Build customer/account cohort segments
4. **Forecast Models**: Predict future performance vs. targets
5. **Cost Efficiency Analysis**: Calculate CAC, ROAS per channel

