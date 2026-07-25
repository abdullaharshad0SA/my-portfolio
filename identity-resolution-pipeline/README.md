# Identity Resolution Pipeline — Log-Level Stitching & IQVIA Enrichment

## Executive Summary

A complex, production-grade pipeline that transforms raw bidstream logs into enriched, properly-identified advertising datasets. The solution bridges internal platform data with client-specific identity infrastructure (proprietary tracker tokens) while handling collision resolution, data quality validation, and secure delivery.

**Business Impact**: Enables high-value client integrations that require granular, identity-enriched advertising data for downstream analytics and machine learning.

## Problem Statement

A critical client requires programmatic, log-level advertising data (raw bidstream records) enriched with their proprietary tracker token: the Experian IQVIA Synthetic Key. However:

1. **Scale**: Processing millions of bidstream event records to match with identity resolution graph
2. **Complexity**: Entity data lives across multiple databases (Redshift, MySQL) requiring orchestration
3. **Quality**: Determining which identity links are reliable vs. ambiguous
4. **Collision Handling**: 6% of identity mappings are 1-to-many (multiple valid identities per device)
5. **Security**: Applying consistent PII censoring before client delivery
6. **Performance**: Delivering compressed files via secure SFTP without resource exhaustion

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   BIDSTREAM EXTRACTION                          │
│         Extract raw logs from Redshift (line_item_id)           │
│   - Timezone normalization (UTC → regional time)                │
│   - Micro-dollar to decimal conversion (÷ 1,000,000)            │
│   - PII censoring (apply censor_pii policy)                     │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│              STRUCTURAL ENRICHMENT (MySQL)                       │
│  - Campaign metadata (names, hierarchies)                        │
│  - Creative and line item descriptors                           │
│  - Segment enrichment (audience categories)                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│           IDENTITY RESOLUTION (PySpark Graph)                    │
│  - Filter identity graph to relevant device IDs                 │
│  - Stage 1: Device ID → Experian LUID mapping                   │
│  - Stage 2: LUID → IQVIA Synthetic Key resolution               │
│  - Collision handling: 1-to-many deterministic random           │
│  - Fill rate tracking (target: 50%, achieved: ~70%)             │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│             FILE SYNTHESIS & DELIVERY                            │
│  - Coalesce Spark DataFrame into single partition               │
│  - Compress to PSV.GZ format (Pipe-Separated Values)            │
│  - Upload to S3 staging, download to /tmp                       │
│  - Secure SFTP delivery to client landing zone                  │
│  - Verification audits (byte size, file timestamps)             │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Bidstream Extraction & Normalization (Redshift)

**Input**: Raw event table (sa_rs_evt_table_YYYY_MM_DD) for target campaign

**Transformations**:
- **Timezone Lookup**: Query operational databases for advertiser's regional account timezone
- **Timezone Translation**: Convert UTC timestamps to localized time (RS_timezone)
- **Micro-Dollar Correction**: `cost_usd = SUM(cost) / 1000000.0`
- **Metric Aggregation**: impressions, clicks, conversions at event level

**Sample Query Pattern**:
```sql
SELECT 
    request_duid_sha256,
    most_associated_duid_sha256,
    campaign_id,
    creative_id,
    SUM(cost) / 1000000.0 AS cost_usd,
    SUM(has_won) AS impressions,
    SUM(has_click) AS clicks,
    SUM(has_conv) AS conversions
FROM sa_rs_evt_table_2024_06_15
WHERE line_item_id = 433722
  AND request_time >= '2024-06-15 00:00:00'
  AND request_time < '2024-06-16 00:00:00'
GROUP BY request_duid_sha256, most_associated_duid_sha256, campaign_id, creative_id
```

### 2. Structural Enrichment (MySQL)

**Joins**: Bidstream records (numeric entity IDs only) with operational metadata

| Source Table | Join Field | Enrichment |
|--------------|-----------|-----------|
| sa_rs_evt | campaign_id → campaigns | campaign_name, campaign_type |
| sa_rs_evt | line_item_id → line_items | line_item_name, status |
| sa_rs_evt | creative_id → creatives | creative_description |
| sa_rs_evt | advertiser_id → sub_advertisers | advertiser_structure |
| sa_rs_evt | segment_id → custom_segments + rt_segments | segment_name (human-readable) |

**Implementation**: INNER JOINs with targeted filtering to avoid cartesian products

### 3. Large-Scale Identity Resolution (PySpark)

**Challenge**: Identity graph (sa_identity) contains millions of records. Loading entire graph → memory exhaustion.

**Solution**: Filter-first, then resolve

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("identity-resolution") \
    .config("spark.driver.memory", "80g") \
    .config("spark.local.dir", "/tmp/spark-spillover") \
    .getOrCreate()

# Extract unique device IDs from bidstream
unique_devices = bidstream_df.select('request_duid_sha256', 'most_associated_duid_sha256').distinct()

# Filter identity graph to relevant entries
identity_graph = spark.read.parquet("s3://data-warehouse/sa_identity/")
filtered_identity = identity_graph.filter(
    col("device_id").isin(unique_devices.collect())
)

# Stage 1: DUID → LUID (Experian Local User ID)
duid_luid = filtered_identity.select('device_id', 'experian_luid').drop_duplicates()

# Stage 2: LUID → IQVIA Key (Synthetic key from latest Experian graph)
experian_graph = spark.read.parquet("s3://experian-graphs/latest/")
iqvia_mapping = experian_graph.select('experian_luid', 'iqvia_synthetic_key')

# Join stages
result = bidstream_df \
    .join(duid_luid, bidstream_df.request_duid_sha256 == duid_luid.device_id, 'left') \
    .join(iqvia_mapping, 'experian_luid', 'left') \
    .select('request_duid_sha256', 'iqvia_synthetic_key', 'cost_usd', 'impressions', 'clicks', 'conversions')
```

### 4. Collision Resolution Strategy

**The Reality**: While 94% of DUID-to-LUID mappings are 1:1, the remaining 6% contain 1-to-many ambiguities (a single device maps to 2, 3, or up to 50 distinct LUID tokens).

**Approved Strategy**: Deterministic random assignment (statistically sound approximation, validated with internal Identity team)

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rand

# Partition by device, apply random ranking with static seed
window = Window.partitionBy('request_duid_sha256').orderBy(rand(seed=42))
collision_resolved = result.withColumn('luid_rank', row_number().over(window)) \
    .filter(col('luid_rank') == 1) \
    .drop('luid_rank')
```

**Justification**: Ensures reproducibility (seed=42) and fairness, while maintaining statistical validity.

## Data Quality Metrics

| Metric | Target | Achieved |
|--------|--------|----------|
| LUID Match Fill Rate | — | 80% |
| IQVIA Synthetic Key Fill Rate | 50% | ~70% |
| 1-to-many Collisions | 6% of mappings | Handled deterministically |

**Note**: Fill rate automatically improves over time as Experian's underlying identity graph ingests more partner matches and machine learning corrections, requiring no code changes.

## File Synthesis & Delivery

### Format Specification
- **Type**: Pipe-Separated Values (PSV), compressed with gzip
- **Filename Convention**: `SA_MediaOS_YYYYMMDD.psv.gz`
- **Partitioning**: Single file per execution date

### Delivery Process
1. Coalesce Spark DataFrame to single partition
2. Serialize to PSV format with pipe delimiter
3. Compress with gzip
4. Upload to S3 staging bucket (`s3://client-deliverables/`)
5. Download to local `/tmp/` cache (secure, containerized)
6. Establish SSH transport bridge via Paramiko
7. Upload to client's SFTP landing zone
8. Stream audit records (byte size, file hash, timestamp) to logging platform

```python
import paramiko
import gzip
from io import BytesIO

# Serialize and compress
buffer = BytesIO()
with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
    gz.write(df.to_csv(index=False, sep='|').encode('utf-8'))
buffer.seek(0)

# SFTP delivery
ssh = paramiko.SSHClient()
ssh.connect(sftp_host, username=sftp_user, key_filename=pem_path)
sftp_client = ssh.open_sftp_client()
sftp_client.putfo(buffer, f'/landing/{filename}.psv.gz')
sftp_client.close()
```

## Orchestration & Monitoring

**Scheduler**: Kestra (daily execution at 10 AM EST)

**Status Tracking**:
- Start/end timestamps logged
- Row counts at each pipeline stage
- Fill rate metrics
- SFTP delivery verification

**Error Handling**:
- Redshift connection failures → Retry with exponential backoff
- MySQL metadata lookups → Log warnings, continue with partial enrichment
- Spark OOM errors → Fail fast with detailed memory diagnostics
- SFTP delivery → Retry up to 3 times, escalate to on-call

## Technical Highlights

### Memory-Efficient Graph Processing
Large identity graphs (100M+ records) processed using:
- Pre-filtering to relevant device IDs
- Window functions instead of full self-joins
- Spill-to-disk configuration for Spark

### PII Compliance
All data passed through `censor_pii()` function:
- Regional restrictions applied (blocks certain countries/US states)
- Device ID hashing for untrusted PII fields
- Cookie consent validation before IP address exposure

### Deterministic Collision Handling
- Seed=42 ensures reproducibility across runs
- Approved statistical method maintains data validity
- Documented in escalation playbooks

## Dependencies

- **Redshift**: Source bidstream tables
- **MySQL**: Entity metadata (campaigns, line items, segments)
- **S3**: Identity graph storage, staging buckets
- **PySpark**: Large-scale identity resolution
- **Paramiko**: SFTP delivery
- **AWS IAM**: Credentials and access control
- **Kestra**: Orchestration and scheduling

## Monitoring & Alerts

| Alert | Threshold | Action |
|-------|-----------|--------|
| LUID Fill Rate Drop | <75% | Notify Data team |
| IQVIA Fill Rate Drop | <65% | Notify Data team + escalate to Experian |
| SFTP Delivery Failure | 3 retries exceeded | Page on-call engineer |
| Micro-dollar normalization errors | > 5 rows | Log warning, quarantine export |

## Future Roadmap

1. **Direct Graph Updates**: Integrate real-time Experian graph updates instead of daily batch
2. **Collision ML Model**: Replace deterministic random with ML-driven collision resolution
3. **Multi-client Scaling**: Parametrize for N clients vs. single-client template
4. **Incremental Delivery**: Stream records instead of batch files for near-real-time updates

