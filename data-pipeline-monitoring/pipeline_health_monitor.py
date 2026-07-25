# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 14:15:14 2026

@author: abdul
"""

from __future__ import annotations
from dataclasses import dataclass

# Iterable allows us to type hint things like lists, tuples, sets, etc.
from typing import Iterable

# NumPy is used only to generate realistic random sample data.
import numpy as np

# Pandas is our primary data analysis library.
import pandas as pd

@dataclass
class CheckResult:
    """
    Stores the result of a single data quality test.

    Every monitoring function returns one of these objects so
    that all results share the same structure.
    """

    # Name of the test performed
    check_name: str

    # Table being tested
    table_name: str

    # pass or fail
    status: str

    # Number of affected rows
    failed_rows: int

    # Human-readable explanation
    detail: str

# SAMPLE DATA CREATION

def build_sample_data(seed: int = 42) -> dict[str, pd.DataFrame]:
    """
    Creates fake data so we can test our monitoring framework.

    In production this data would come from:
        - Snowflake
        - Redshift
        - BigQuery
        - Databricks
        - CSV files
        - APIs

    Here we generate everything in memory.
    """

    # Using a seed guarantees reproducible random data.
    rng = np.random.default_rng(seed)

    # Today's date with the time removed.
    today = pd.Timestamp.today().normalize()

    # Generate account IDs:
    # acct_0001
    # acct_0002
    account_ids = [
        f"acct_{i:04d}"
        for i in range(1, 81)
    ]

    # Generate opportunity IDs:
    # opp_00001
    # opp_00002
    opportunity_ids = [
        f"opp_{i:05d}"
        for i in range(1, 181)
    ]

    # Accounts table

    accounts = pd.DataFrame(
        {
            "account_id": account_ids,
            "account_name": [
                f"Sample Account {i}"
                for i in range(1, 81)
            ],

            # Randomly assign account segments.
            "segment": rng.choice(
                ["Enterprise", "Mid-Market", "SMB"],
                size=80,
                p=[0.25, 0.45, 0.30],
            ),

            # Randomly assign regions.
            "region": rng.choice(
                ["North America", "Europe", "APAC"],
                size=80,
                p=[0.65, 0.25, 0.10],
            ),

            # Simulate records being updated recently.
            "updated_at": (
                today
                - pd.to_timedelta(
                    rng.integers(0, 4, size=80),
                    unit="D",
                )
            ),
        }
    )

    # Opportunities table

    opportunities = pd.DataFrame(
        {
            "opportunity_id": opportunity_ids,

            # Randomly assign opportunities to accounts.
            "account_id": rng.choice(
                account_ids,
                size=180,
            ),

            # Randomly assign sales stages.
            "stage_name": rng.choice(
                [
                    "Prospecting",
                    "Discovery",
                    "Evaluation",
                    "Negotiation",
                    "Closed Won",
                    "Closed Lost",
                    "Bad Stage",  # intentionally invalid
                ],
                size=180,
                p=[
                    0.16,
                    0.18,
                    0.22,
                    0.14,
                    0.16,
                    0.12,
                    0.02, # Rare 
                ],
            ),

            # Revenue amount.
            "amount_reporting": (
                rng.integers(
                    5_000,
                    150_000,
                    size=180,
                ).astype(float)
            ),

            "updated_at": (
                today
                - pd.to_timedelta(
                    rng.integers(0, 5, size=180),
                    unit="D",
                )
            ),
        }
    )

    # Inject intentional data issues

    # Duplicate primary key.
    opportunities.loc[5, "opportunity_id"] = (
        opportunities.loc[4, "opportunity_id"]
    )

    # Missing foreign key.
    opportunities.loc[12, "account_id"] = None

    # Missing revenue value.
    opportunities.loc[25, "amount_reporting"] = np.nan

    # Revenue source table

    revenue_source = (
        opportunities[
            ["opportunity_id", "amount_reporting"]
        ]
        .copy()
        .rename(
            columns={
                "amount_reporting": "amount_source"
            }
        )
    )

    revenue_source["amount_source"] = (
        revenue_source["amount_source"]
        .fillna(0)
    )

    # Create intentional reconciliation issues.
    mismatch_rows = (
        revenue_source
        .sample(8, random_state=seed) # Randomly pick 8 rows. 
        .index
    )

    revenue_source.loc[
        mismatch_rows,
        "amount_source",
    ] += rng.integers(
        -2500,
        3500,
        size=len(mismatch_rows),
    )

    # Historical row count snapshots
    row_count_history = pd.DataFrame(
        {
            "table_name": ["opportunities"] * 7,

            "snapshot_date": pd.date_range(
                today - pd.Timedelta(days=6),
                today,
                freq="D",
            ),

            "row_count": [
                172,
                174,
                177,
                179,
                181,
                180,
                139,
            ],
        }
    )

    return {
        "accounts": accounts,
        "opportunities": opportunities,
        "revenue_source": revenue_source,
        "row_count_history": row_count_history,
    }

# REQUIRED FIELD TEST

# checks if col exists
# checks col contains no nulls
def check_required_fields(
    df: pd.DataFrame,
    table_name: str,
    required_fields: Iterable[str],
) -> list[CheckResult]:

    results: list[CheckResult] = []

    for field in required_fields:

        # Schema validation
        if field not in df.columns:

            results.append(
                CheckResult(
                    check_name="required_field_exists",
                    table_name=table_name,
                    status="fail",
                    failed_rows=len(df),
                    detail=f"{field} is missing.",
                )
            )

            continue

        # Data validation
        missing_count = int(
            df[field]
            .isna()
            .sum()
        )

        results.append(
            CheckResult(
                check_name="required_field_not_null",
                table_name=table_name,
                status=(
                    "pass"
                    if missing_count == 0
                    else "fail"
                ),
                failed_rows=missing_count,
                detail=(
                    f"{field} has "
                    f"{missing_count} missing value(s)."
                ),
            )
        )

    return results

# DUPLICATE KEY TEST

def check_duplicate_key(
    df: pd.DataFrame,
    table_name: str,
    key_column: str,
) -> CheckResult:

    duplicate_count = int(
        df.duplicated(
            subset=[key_column],
            keep=False,
        ).sum()
    )

    return CheckResult(
        check_name="duplicate_primary_key",
        table_name=table_name,
        status=(
            "pass"
            if duplicate_count == 0
            else "fail"
        ),
        failed_rows=duplicate_count,
        detail=(
            f"{key_column} has "
            f"{duplicate_count} duplicated row(s)."
        ),
    )


# FRESHNESS TEST

def check_freshness(
    df: pd.DataFrame,
    table_name: str,
    timestamp_column: str,
    max_age_days: int,
) -> CheckResult:

    newest_record = pd.to_datetime(
        df[timestamp_column]
    ).max()

    age_days = (
        pd.Timestamp.today().normalize()
        - newest_record.normalize()
    ).days

    return CheckResult(
        check_name="freshness",
        table_name=table_name,
        status=(
            "pass"
            if age_days <= max_age_days
            else "fail"
        ),
        failed_rows=(
            0
            if age_days <= max_age_days
            else len(df)
        ),
        detail=(
            f"Newest record is "
            f"{age_days} day(s) old."
        ),
    )


# VALID VALUES TEST

def check_valid_values(
    df: pd.DataFrame,
    table_name: str,
    column: str,
    valid_values: set[str],
) -> CheckResult:

    invalid_rows = int(
        (~df[column].isin(valid_values)).sum()
    )

    invalid_values = sorted(
        df.loc[
            ~df[column].isin(valid_values),
            column,
        ]
        .dropna()
        .unique()
        .tolist()
    )

    return CheckResult(
        check_name="valid_values",
        table_name=table_name,
        status=(
            "pass"
            if invalid_rows == 0
            else "fail"
        ),
        failed_rows=invalid_rows,
        detail=(
            f"{column} has invalid values: "
            f"{invalid_values}"
        ),
    )


# REVENUE RECONCILIATION TEST

def check_revenue_reconciliation(
    opportunities: pd.DataFrame,
    revenue_source: pd.DataFrame,
):

    joined = opportunities.merge(
        revenue_source,
        on="opportunity_id",
        how="left",
    )

    joined["amount_reporting"] = (
        joined["amount_reporting"]
        .fillna(0)
    )

    joined["amount_source"] = (
        joined["amount_source"]
        .fillna(0)
    )

    joined["revenue_difference"] = (
        joined["amount_reporting"]
        - joined["amount_source"]
    )

    mismatch_rows = joined[
        joined["revenue_difference"].abs() > 1
    ].copy()

    result = CheckResult(
        check_name="revenue_reconciliation",
        table_name="opportunities",
        status=(
            "pass"
            if mismatch_rows.empty
            else "fail"
        ),
        failed_rows=len(mismatch_rows),
        detail=(
            f"{len(mismatch_rows)} "
            f"revenue mismatches found."
        ),
    )

    return result, mismatch_rows


# ROW COUNT ANOMALY TEST

def check_row_count_drop(
    row_count_history: pd.DataFrame,
    threshold_pct: float = 0.20,
) -> CheckResult:

    history = (
        row_count_history
        .sort_values("snapshot_date")
        .copy()
    )

    previous_count = float(
        history.iloc[-2]["row_count"]
    )

    current_count = float(
        history.iloc[-1]["row_count"]
    )

    drop_pct = (
        (previous_count - current_count)
        / previous_count
        if previous_count
        else 0
    )

    return CheckResult(
        check_name="row_count_drop",
        table_name="opportunities",
        status=(
            "pass"
            if drop_pct <= threshold_pct
            else "fail"
        ),
        failed_rows=(
            0
            if drop_pct <= threshold_pct
            else int(current_count)
        ),
        detail=(
            f"Row count changed by "
            f"{drop_pct:.1%}"
        ),
    )


# REPORTING

def print_monitor_report(
    results_df: pd.DataFrame,
    revenue_mismatches: pd.DataFrame,
):

    total_checks = len(results_df)

    failed_checks = int(
        (results_df["status"] == "fail").sum()
    )

    passed_checks = total_checks - failed_checks

    score = (
        round(
            (passed_checks / total_checks) * 100,
            1,
        )
        if total_checks
        else 0
    )

    print("\n" + "=" * 100)
    print("REPORTING HEALTH MONITOR")
    print("=" * 100)

    print(f"Health Score : {score}%")
    print(f"Passed Checks: {passed_checks}")
    print(f"Failed Checks: {failed_checks}")

    failed_df = results_df[
        results_df["status"] == "fail"
    ]

    print("\nFAILED CHECKS")

    if failed_df.empty:
        print("✓ No failed checks.")
    else:
        print(
            failed_df.to_string(index=False)
        )

    print("\nREVENUE MISMATCHES")

    if revenue_mismatches.empty:
        print("✓ No mismatches.")
    else:
        print(
            revenue_mismatches.head(20)
            .to_string(index=False)
        )


# Load data
data = build_sample_data()

accounts = data["accounts"]
opportunities = data["opportunities"]
revenue_source = data["revenue_source"]
row_count_history = data["row_count_history"]

# Collect all test results
results: list[CheckResult] = []

# Required field tests
results.extend(
    check_required_fields(
        accounts,
        "accounts",
        [
            "account_id",
            "account_name",
            "segment",
            "updated_at",
        ],
    )
)

results.extend(
    check_required_fields(
        opportunities,
        "opportunities",
        [
            "opportunity_id",
            "account_id",
            "stage_name",
            "amount_reporting",
            "updated_at",
        ],
    )
)

# Duplicate key tests
results.append(
    check_duplicate_key(
        accounts,
        "accounts",
        "account_id",
    )
)

results.append(
    check_duplicate_key(
        opportunities,
        "opportunities",
        "opportunity_id",
    )
)

# Freshness tests
results.append(
    check_freshness(
        accounts,
        "accounts",
        "updated_at",
        2,
    )
)

results.append(
    check_freshness(
        opportunities,
        "opportunities",
        "updated_at",
        2,
    )
)

# Valid value test
valid_stages = {
    "Prospecting",
    "Discovery",
    "Evaluation",
    "Negotiation",
    "Closed Won",
    "Closed Lost",
}

results.append(
    check_valid_values(
        opportunities,
        "opportunities",
        "stage_name",
        valid_stages,
    )
)

# Revenue reconciliation
revenue_check, revenue_mismatches = (
    check_revenue_reconciliation(
        opportunities,
        revenue_source,
    )
)

results.append(revenue_check)

# Row count anomaly
results.append(
    check_row_count_drop(
        row_count_history,
        threshold_pct=0.20,
    )
)

# Convert all results into a DataFrame
results_df = pd.DataFrame(
    [r.__dict__ for r in results]
)

# Print report
print_monitor_report(
    results_df,
    revenue_mismatches,
)
