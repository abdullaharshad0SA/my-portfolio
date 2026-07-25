# Reach & Frequency Analysis — Audience Saturation Metrics

## Problem Statement

In digital advertising, basic metrics like gross impression counts fail to reveal actual audience saturation. Campaign health requires understanding:

- **Reach**: How many unique individuals saw the ad?
- **Frequency**: How many times did the average person see it?
- **Distribution**: What percentage of the audience saw it exactly 1 time, 5 times, 20+ times?

**Business Challenge**: Without these metrics, ad operations teams cannot:
- Detect ad fatigue (frequency too high)
- Optimize budget efficiency
- Make data-driven creative rotation decisions
- Prevent diminishing returns from overexposure

## Solution Overview

An automated framework that processes raw, log-level advertising data (bidstream logs) to compute:

1. **Deduplicated Reach**: Count of unique users exposed
2. **Average Frequency**: Total impressions ÷ unique users
3. **Frequency Distribution**: Exact-count buckets (1x, 2x, 3x) AND cumulative thresholds (1+, 5+, 10+, 20+, 50+)
4. **Weekly Trend Analysis**: How saturation evolves across campaign flight
5. **Segment Breakdown**: Reach/frequency by creative, campaign, line item

## Architecture

```
┌─────────────────────────────────────────────────┐
│  PHASE A: Dynamic Localized Extraction          │
│  - Detect execution platform (EC2 vs. Local)    │
│  - Load timezone translation matrix             │
│  - Daily chunking loop to extract millions rows │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  PHASE B: Relational Metadata Enrichment        │
│  - Join raw logs to campaign/line item names    │
│  - Enrich with operational metadata             │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  PHASE C: Two-Tier Frequency Modeling           │
│  - User-level: Count impressions per user       │
│  - Campaign-level: Compute reach, frequency     │
│  - Bucketing: Exact counts AND cumulative       │
└─────────────────────────────────────────────────┘
```

## Core Components

### Phase A: Dynamic Localized Extraction & Ingestion Loops

#### Server Detection & Path Configuration
```python
import platform
import sys
import os

sysname = platform.system()

if sysname == 'Linux':
    # Production EC2 environment
    credential_path = '/home/ec2-user/pa-lead/nomad/Common_Artifacts_Python'
    sys.path.append(os.path.abspath(credential_path))
    from pyKeys import *
    from Common_Functions import *
else:
    # Local development environment
    from local_credentials import *
```

#### Timezone Translation Matrix
```python
def translate_to_timezone(utc_timestamp, target_timezone):
    """Convert UTC timestamp to target timezone."""
    import pytz
    
    utc_tz = pytz.UTC
    target_tz = pytz.timezone(target_timezone)
    
    utc_time = utc_tz.localize(utc_timestamp)
    local_time = utc_time.astimezone(target_tz)
    
    return local_time
```

**Context**: Ad servers capture interactions globally in UTC. This script translates to localized business-day partitions.

#### Horizontal Daily Looper
```python
def daily_looper_extract(line_item_ids, start_date, end_date, timezone='US/Eastern'):
    """
    Incrementally extract raw logs without memory exhaustion.
    Partition by daily boundaries with timezone translation.
    """
    from datetime import datetime, timedelta
    import pandas as pd
    
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    all_data = []
    
    while current_date <= end:
        # Translate UTC partition boundaries to local timezone
        local_start = translate_to_timezone(
            current_date,
            timezone
        )
        local_end = local_start + timedelta(days=1)
        
        # Query single day's events
        query = f"""
        SELECT 
            request_duid_sha256 AS user_id,
            line_item_id,
            campaign_id,
            creative_id,
            SUM(has_won) AS impressions
        FROM sa_rs_evt_table_{current_date.strftime('%Y_%m_%d')}
        WHERE line_item_id IN ({','.join(map(str, line_item_ids))})
          AND request_time >= '{local_start}'
          AND request_time < '{local_end}'
        GROUP BY request_duid_sha256, line_item_id, campaign_id, creative_id
        """
        
        # Execute and append (no memory accumulation)
        daily_df = pd.read_sql(query, engine)
        all_data.append(daily_df)
        
        current_date += timedelta(days=1)
    
    # Combine only at the end
    return pd.concat(all_data, ignore_index=True)
```

**Key Benefit**: Chunking by date prevents memory exhaustion with millions of rows.

### Phase B: Relational Metadata Enrichment

```python
def enrich_with_metadata(raw_logs_df, db_connection):
    """
    Enhance raw numeric entity IDs with human-readable names.
    """
    
    # Query metadata for line items
    line_item_query = """
    SELECT line_item_id, name, status, advertiser_id
    FROM line_items
    """
    line_items = pd.read_sql(line_item_query, db_connection)
    
    # Left-merge to preserve all raw logs
    enriched = raw_logs_df.merge(
        line_items,
        on='line_item_id',
        how='left'
    )
    
    return enriched
```

**Output**: Each raw event record now includes campaign names, line item descriptors, advertiser hierarchies.

### Phase C: Two-Tier Frequency Modeling Matrix

#### Tier 1: User-Level Aggregation
```python
def compute_user_frequency(enriched_df):
    """
    For each unique user+campaign pair, count total exposures.
    """
    user_freq = enriched_df.groupby(
        ['campaign_id', 'campaign_name', 'user_id']
    ).agg({
        'impressions': 'sum'
    }).reset_index()
    
    # Rename for clarity
    user_freq.rename(
        columns={'impressions': 'user_impressions'},
        inplace=True
    )
    
    return user_freq
```

**Output**:
```
campaign_id  campaign_name  user_id           user_impressions
433722       "Q2 Promo"     device_abc123     5
433722       "Q2 Promo"     device_def456     12
433722       "Q2 Promo"     device_ghi789     2
```

#### Tier 2: Campaign-Level Rollup with Bucketing
```python
def compute_frequency_distribution(user_freq):
    """
    From user-level frequencies, compute campaign-level metrics
    and frequency distribution buckets.
    """
    
    results = []
    
    for campaign_id, group in user_freq.groupby('campaign_id'):
        campaign_name = group['campaign_name'].iloc[0]
        
        # Metric 1: Reach (unique users)
        reach = len(group)
        
        # Metric 2: Total impressions
        total_imps = group['user_impressions'].sum()
        
        # Metric 3: Average frequency
        avg_frequency = total_imps / reach
        
        # Metric 4: Exact-count distribution
        # (How many users saw ad exactly 1x, 2x, 3x, etc.)
        freq_counts = group['user_impressions'].value_counts().sort_index()
        
        exact_buckets = {}
        for freq_count, user_count in freq_counts.items():
            exact_buckets[f'{freq_count}x'] = user_count
        
        # Metric 5: Cumulative thresholds
        # (How many users saw ad 1+, 5+, 10+ times)
        cumulative_buckets = {}
        for threshold in [1, 5, 10, 20, 50]:
            users_at_threshold = (group['user_impressions'] >= threshold).sum()
            cumulative_buckets[f'{threshold}+'] = users_at_threshold
        
        results.append({
            'campaign_id': campaign_id,
            'campaign_name': campaign_name,
            'reach': reach,
            'total_impressions': total_imps,
            'avg_frequency': round(avg_frequency, 2),
            **exact_buckets,
            **cumulative_buckets
        })
    
    return pd.DataFrame(results)
```

**Output**:
```
campaign_id  campaign_name  reach  total_imps  avg_freq  1x  2x  3x  ...  1+  5+  10+  20+
433722       "Q2 Promo"     1000   5500       5.50       300 250 200 ...  1000 450  200  50
```

## Example Analysis

### Dataset
- 1 campaign
- 1,000 unique users
- 5,500 total impressions
- Flight duration: June 1-30

### Metrics
| Metric | Value |
|--------|-------|
| Reach | 1,000 users |
| Avg Frequency | 5.5 impressions/user |
| Users with 1+ exposure | 1,000 (100%) |
| Users with 5+ exposure | 450 (45%) |
| Users with 10+ exposure | 200 (20%) |
| Users with 20+ exposure | 50 (5%) |
| Users with 50+ exposure | 5 (<1%) |

### Strategic Insights
1. **Ad Fatigue Risk**: 5% of audience exceeded 20 impressions (potential diminishing returns)
2. **Core Audience**: 45% saw ad 5+ times (engaged audience)
3. **Frequency Distribution**: Relatively balanced (not heavily skewed)
4. **Optimization**: Consider creative rotation or frequency capping at 15 impressions

## Weekly Trend Analysis

```python
def compute_weekly_trends(enriched_df, campaign_id):
    """Analyze reach/frequency evolution across campaign flight."""
    
    enriched_df['week'] = pd.to_datetime(enriched_df['request_time']).dt.isocalendar().week
    
    weekly_trends = enriched_df.groupby('week').apply(
        lambda week_data: compute_frequency_distribution(
            compute_user_frequency(week_data)
        )
    )
    
    return weekly_trends
```

**Output**: Track how saturation increases or decreases week-by-week.

## Integration & Automation

### Scheduling
```python
# Kestra orchestration (weekly execution, Sunday 2 AM)
def scheduled_reach_frequency():
    """Production-grade reach & frequency calculation."""
    
    # Extract past 7 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    # Run pipeline
    raw_logs = daily_looper_extract(
        line_item_ids=[433722],
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        timezone='US/Eastern'
    )
    
    enriched = enrich_with_metadata(raw_logs, db_connection)
    user_freq = compute_user_frequency(enriched)
    distribution = compute_frequency_distribution(user_freq)
    
    # Publish to dashboard
    publish_to_tableau(distribution)
```

## Roadmap: Real-Time Audience Microservice

**Goal**: Eliminate manual configuration, establish true self-sustaining analytics.

### Phase 1: Direct Warehouse Integration (Current)
- Manual parameter inputs
- Weekly batch execution

### Phase 2: Self-Service API (Planned)
```python
@app.route('/reach-frequency/<campaign_id>/', methods=['GET'])
def get_campaign_saturation(campaign_id):
    """
    Real-time reach & frequency for any campaign.
    """
    result = compute_frequency_distribution(
        compute_user_frequency(
            enrich_with_metadata(
                daily_looper_extract(
                    line_item_ids=[campaign_id],
                    start_date=request.args.get('start_date'),
                    end_date=request.args.get('end_date')
                )
            )
        )
    )
    return result.to_json()
```

### Phase 3: Live Dashboard Feed
- Real-time metric updates
- Automatic recommendations (creative rotation, frequency caps)
- Customer-facing saturation reports

## Technical Highlights

- **Scalable Log Processing**: Handles millions of records via daily chunking
- **Timezone-Aware**: Respects regional business days
- **Multi-Granularity Reporting**: Exact counts + cumulative thresholds
- **Operational Efficiency**: 60-minute execution, no manual intervention
- **Data Driven**: Enables quantitative optimization vs. guesswork

## Dependencies

- Redshift (bidstream tables)
- MySQL (metadata: campaigns, line items)
- Pandas (aggregation, bucketing)
- Kestra (orchestration)

## Monitoring & Alerts

| Metric | Alert Threshold | Action |
|--------|-----------------|--------|
| Avg Frequency > 15 | WARNING | Consider creative rotation |
| 50%+ users at 10+ freq | WARNING | Risk of audience saturation |
| Reach < 100 | INFO | Campaign underperforming delivery |

