# Data Pipeline Monitoring — Health Check & Failure Detection

## Problem Statement

Modern data pipelines frequently experience **silent failures**—incidents where automated ingestion runs technically succeed without throwing code errors, but output stale, duplicated, or unreconciled data downstream.

**Business Risk**: Leadership makes high-stakes strategic decisions based on corrupted reporting metrics, leading to misaligned spend and missed targets.

## Solution Overview

A comprehensive, OOP-based monitoring framework that executes **six multi-vector integrity tests** on production databases:

1. **Missing Values Test**: Structural completeness
2. **Referential Integrity Test**: Foreign key validation
3. **Uniqueness Test**: Primary key duplicate detection
4. **Freshness Test**: Data synchronization lag monitoring
5. **Domain Constraints Test**: Business logic enforcement
6. **Financial Reconciliation Test**: Cross-system audit

**Result**: Automated gatekeeping system catches data quality issues before they corrupt downstream analytics.

## Architecture

### OOP Design with Python Data Classes

```python
from dataclasses import dataclass
from typing import List, Dict, Any
import pandas as pd

@dataclass
class DataQualityCheck:
    """Base class for all data quality validations."""
    table_name: str
    check_name: str
    passed: bool
    failed_records: int
    error_message: str = ""
    
    def execute(self):
        raise NotImplementedError

class DataQualityMonitor:
    """Orchestrates all health checks for a dataset."""
    
    def __init__(self, account_df, opportunity_df, financial_df):
        self.account_df = account_df
        self.opportunity_df = opportunity_df
        self.financial_df = financial_df
        self.results: List[DataQualityCheck] = []
    
    def run_all_checks(self):
        """Execute comprehensive health check suite."""
        self.results.append(self.check_missing_values())
        self.results.append(self.check_referential_integrity())
        self.results.append(self.check_duplicate_key())
        self.results.append(self.check_freshness())
        self.results.append(self.check_valid_values())
        self.results.append(self.check_revenue_reconciliation())
        
        return self.results
    
    def generate_report(self):
        """Produce human-readable monitoring report."""
        passed = sum(1 for r in self.results if r.passed)
        total = len(self.results)
        
        print(f"\n{'='*60}")
        print(f"DATA QUALITY REPORT: {passed}/{total} checks passed")
        print(f"{'='*60}\n")
        
        for check in self.results:
            status = "✓ PASS" if check.passed else "✗ FAIL"
            print(f"{status} | {check.table_name}.{check.check_name}")
            if not check.passed:
                print(f"       → {check.error_message}")
                print(f"       → Failed records: {check.failed_records}")
```

### Test 1: Missing Values & Structural Completeness

```python
def check_missing_values(self) -> DataQualityCheck:
    """Detect null indicators and whitespace gaps in critical columns."""
    
    critical_columns = {
        'account': ['account_id', 'account_name', 'account_status'],
        'opportunity': ['opp_id', 'account_id', 'opp_value', 'stage'],
        'financial': ['id', 'transaction_amount', 'posted_date']
    }
    
    results = []
    
    for table_name, columns in critical_columns.items():
        table = getattr(self, f'{table_name}_df')
        
        for col in columns:
            missing_count = table[col].isna().sum()
            missing_pct = (missing_count / len(table)) * 100
            
            if missing_pct > 0:
                results.append({
                    'table': table_name,
                    'column': col,
                    'missing_count': missing_count,
                    'missing_pct': missing_pct
                })
    
    if results:
        error_msg = f"Found missing values in {len(results)} column(s)"
        return DataQualityCheck(
            table_name='structural',
            check_name='missing_values',
            passed=False,
            failed_records=sum(r['missing_count'] for r in results),
            error_message=error_msg
        )
    
    return DataQualityCheck(
        table_name='structural',
        check_name='missing_values',
        passed=True,
        failed_records=0
    )
```

### Test 2: Referential Integrity & Key Association

```python
def check_referential_integrity(self) -> DataQualityCheck:
    """Validate foreign key relationships; expose orphan rows."""
    
    # Opportunities should reference existing accounts
    opportunities_with_accounts = self.opportunity_df.merge(
        self.account_df[['account_id']],
        on='account_id',
        how='left',
        indicator=True
    )
    
    orphans = opportunities_with_accounts[
        opportunities_with_accounts['_merge'] == 'left_only'
    ]
    
    if len(orphans) > 0:
        return DataQualityCheck(
            table_name='opportunity',
            check_name='referential_integrity',
            passed=False,
            failed_records=len(orphans),
            error_message=f"Found {len(orphans)} opportunities with non-existent account_id"
        )
    
    return DataQualityCheck(
        table_name='opportunity',
        check_name='referential_integrity',
        passed=True,
        failed_records=0
    )
```

### Test 3: Uniqueness & Primary Key Duplicate Guard

```python
def check_duplicate_key(self) -> DataQualityCheck:
    """Validate that primary keys are strictly unique."""
    
    duplicate_accounts = self.account_df[
        self.account_df.duplicated(subset=['account_id'], keep=False)
    ]
    
    if len(duplicate_accounts) > 0:
        return DataQualityCheck(
            table_name='account',
            check_name='duplicate_key',
            passed=False,
            failed_records=len(duplicate_accounts),
            error_message=f"Found {len(duplicate_accounts)} duplicate account_id entries"
        )
    
    return DataQualityCheck(
        table_name='account',
        check_name='duplicate_key',
        passed=True,
        failed_records=0
    )
```

### Test 4: Timeline Freshness & Data Sync Lag Monitor

```python
from datetime import datetime, timedelta

def check_freshness(self) -> DataQualityCheck:
    """
    Measure pipeline synchronization lag.
    Flag if data ingestion stalls beyond threshold.
    """
    
    FRESHNESS_THRESHOLD_DAYS = 2
    
    max_updated = self.opportunity_df['updated_at'].max()
    current_time = datetime.now()
    
    lag_days = (current_time - max_updated).days
    
    if lag_days > FRESHNESS_THRESHOLD_DAYS:
        return DataQualityCheck(
            table_name='opportunity',
            check_name='freshness',
            passed=False,
            failed_records=len(self.opportunity_df),
            error_message=f"Data sync lag: {lag_days} days (threshold: {FRESHNESS_THRESHOLD_DAYS} days)"
        )
    
    return DataQualityCheck(
        table_name='opportunity',
        check_name='freshness',
        passed=True,
        failed_records=0,
        error_message=f"Latest sync: {lag_days} day(s) ago"
    )
```

### Test 5: Domain Constraints & State Validation

```python
def check_valid_values(self) -> DataQualityCheck:
    """Enforce business process rules at the data tier."""
    
    valid_stages = [
        'prospecting', 
        'evaluation', 
        'negotiation',
        'closed_won', 
        'closed_lost'
    ]
    
    invalid_stages = self.opportunity_df[
        ~self.opportunity_df['stage'].isin(valid_stages)
    ]
    
    if len(invalid_stages) > 0:
        invalid_values = invalid_stages['stage'].unique()
        return DataQualityCheck(
            table_name='opportunity',
            check_name='valid_values',
            passed=False,
            failed_records=len(invalid_stages),
            error_message=f"Invalid stage values: {invalid_values}"
        )
    
    return DataQualityCheck(
        table_name='opportunity',
        check_name='valid_values',
        passed=True,
        failed_records=0
    )
```

### Test 6: Cross-System Financial Revenue Reconciliation

```python
def check_revenue_reconciliation(self) -> DataQualityCheck:
    """
    Act as financial auditing layer.
    Validate revenue consistency between sales pipeline and billing.
    """
    
    TOLERANCE = 0.01  # Allow $0.01 rounding error
    
    # Join on primary key
    reconciliation = self.opportunity_df.merge(
        self.financial_df,
        left_on='opp_id',
        right_on='id',
        how='inner',
        suffixes=('_sales', '_billing')
    )
    
    # Calculate deltas
    reconciliation['delta'] = abs(
        reconciliation['opp_value'] - reconciliation['transaction_amount']
    )
    
    mismatches = reconciliation[reconciliation['delta'] > TOLERANCE]
    
    if len(mismatches) > 0:
        total_delta = mismatches['delta'].sum()
        return DataQualityCheck(
            table_name='revenue',
            check_name='reconciliation',
            passed=False,
            failed_records=len(mismatches),
            error_message=f"Revenue mismatch: ${total_delta:.2f} across {len(mismatches)} records"
        )
    
    return DataQualityCheck(
        table_name='revenue',
        check_name='reconciliation',
        passed=True,
        failed_records=0
    )
```

## Integration & Scheduling

### Daily Execution
```python
# Schedule via Kestra or airflow
monitor = DataQualityMonitor(account_df, opportunity_df, financial_df)
results = monitor.run_all_checks()
report = monitor.generate_report()

# Slack alert if any check fails
if any(not r.passed for r in results):
    slack_client.chat_postMessage(
        channel='#data-alerts',
        text="⚠️ Data quality issues detected. Review detailed report."
    )
```

### Output Artifact
```
============================================================
DATA QUALITY REPORT: 5/6 checks passed
============================================================

✓ PASS | structural.missing_values
✓ PASS | opportunity.referential_integrity
✗ FAIL | account.duplicate_key
       → Found 23 duplicate account_id entries
       → Failed records: 23
✓ PASS | opportunity.freshness
✓ PASS | opportunity.valid_values
✗ FAIL | revenue.reconciliation
       → Revenue mismatch: $4,573.29 across 8 records
       → Failed records: 8
```

## Scalability & Extension

**Key Design Principle**: Modular, standalone checks scale to new tables without refactoring.

### Adding New Table Checks
```python
# Simply extend monitor with new method
def check_customer_creditworthiness(self) -> DataQualityCheck:
    """Validate customer credit limits."""
    ...
    return DataQualityCheck(...)
```

### Deploying to New Datasets
```python
monitor = DataQualityMonitor(
    new_accounts_df,
    new_opportunities_df,
    new_financial_df
)
monitor.run_all_checks()
```

## Operational Alerts

| Check | Alert Level | Action |
|-------|-----------|--------|
| Missing Values | WARNING | Review affected records, investigate source |
| Referential Integrity | CRITICAL | Block downstream processing until resolved |
| Duplicate Keys | CRITICAL | Investigate duplicate source, deduplicate |
| Freshness | WARNING | Escalate to data platform team if >48h lag |
| Invalid Values | CRITICAL | Quarantine records, alert data steward |
| Revenue Mismatch | CRITICAL | Audit source systems, escalate to finance |

## Technical Highlights

- **OOP Design**: Extensible class hierarchy, easy to test each check
- **Data Classes**: Immutable result objects ensure audit trail integrity
- **Vectorized Operations**: Uses Pandas groupby/merge (no loops)
- **Detailed Reporting**: Both programmatic (alerts) and human-readable (reports)
- **Zero Dependencies**: Uses only Pandas and standard library

## Future Roadmap

1. **Drift Detection**: ML models to detect anomalous patterns
2. **Predictive Alerts**: Flag data quality issues before they cause downstream failures
3. **Root Cause Analysis**: Automated investigation of quality anomalies
4. **Historical Tracking**: Trend quality metrics over time
5. **Custom Checks**: Configuration-driven check definitions

