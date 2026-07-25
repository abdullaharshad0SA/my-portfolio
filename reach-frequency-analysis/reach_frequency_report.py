#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  8 11:39:13 2025

@author: abdullah.arshad
"""

import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from io import StringIO
import os
import sys
import platform
import getpass
from sqlalchemy import create_engine, text
import datetime
from datetime import datetime, date, timedelta
import re
import numpy as np
import csv


#------------- Configuration
sysname = platform.uname()[0]
if platform.system() == 'Linux':
  print("Server")
  
  # Import Common Functions
  sys.path.append(os.path.abspath("/home/ec2-user/pa-lead/python_schedule/"))
  sys.path.append(os.path.abspath("/home/ec2-user/pa-lead/"))
  
  from functions import *
  from Query_Builder import QueryBuilder
  from Common_Functions import *
else:
    sys.path.append(os.path.abspath("/Users/abdullah.arshad/Documents/GitHub/analytics-reporting/Python Scripts/SA QueryBuilder"))
    from src.Query_Builder import QueryBuilder
    from src.Common_Functions import *


metrics = [
    "sum(cost)/1000000.0 as cost",
    "sum(has_won) as has_won", 
    "sum(has_click) as has_click"
]

where_clause = "AND has_won = 1"

user_id = 36353
advertiser = 106412
cg = 373706, 373705, 373704, 373703, 373702, 373698, 373696, 373695, 373692, 373691, 373689, 373687
user_name = 'cumm_rf_arena'
user_timezone = 'US/Eastern'
start_date = "2025-07-30"
end_date = "2025-09-05" 

filter_ids = cg
filter_column = 'line_item_id'  

def get_lineitem_mysql_info():
    mydb = mydb_conn_func()
    mydb_q = f"""
    SELECT 
    line_items.id AS cg_id,
    line_items.name AS cg_name
    FROM
        line_items
    WHERE line_items.user_id IN ({user_id})
    GROUP by line_items.id """
    mysql_df = pd.read_sql(mydb_q, con = mydb)
    mydb.dispose()
    return mysql_df

def get_rs_data(start_date_temp, end_date_temp):
    print(start_date_temp)
    print(end_date_temp)
    dur = (datetime.strptime(end_date_temp,"%Y-%m-%d %H:%M:%S") - datetime.strptime(start_date_temp,"%Y-%m-%d %H:%M:%S")).days +2
    print("Connecting to DB Engine")
    final_data = pd.DataFrame()
    # Get data from Redshift
    SELECT_stmnt = f"""SELECT
    date_trunc('day',convert_timezone('{user_timezone}',request_time)) as local_date,
    line_item_id as cg_id,
    uniq_imp_id,
    has_won"""
    WHERE_stmnt_non_date = f"WHERE {filter_column} IN {filter_ids} and has_won>0 "
    WHERE_stmnt = WHERE_stmnt_non_date+" AND request_time >= '"+start_date_temp+"' AND request_time < '"+end_date_temp+"'"
    GROUPBY = " "
    ORDERBY = " "
    print('Data Pull now!')
    print(WHERE_stmnt)
    end_date_for_looper = pd.to_datetime(end_date_utc).strftime("%Y-%m-%d")
    start_date_for_looper = pd.to_datetime(start_date_utc).strftime("%Y-%m-%d")  # if the looper also uses start
    if (sysname == 'Linux'):
        tbl_pivot1 = daily_looper_fun(dur,end_date_temp,SELECT_stmnt,WHERE_stmnt,GROUPBY,ORDERBY,user_id)
    else:
        tbl_pivot1 = daily_looper_fun(dur,end_date_for_looper,SELECT_stmnt,WHERE_stmnt,GROUPBY,ORDERBY,user_id)
    print(tbl_pivot1)
    print("Group by completed")
    return tbl_pivot1

start_date = f'{start_date} 00:00:00'
end_date = f'{end_date} 23:59:59'
print(f'{start_date} to {end_date}') 
utc_times = utc_translation(start_date,end_date,user_timezone)
start_date_utc = utc_times[0]
end_date_utc = utc_times[1]

df = get_rs_data(start_date_utc, end_date_utc)
df = df.copy()
line_info = get_lineitem_mysql_info()
df = df.merge(line_info,how='left',on='cg_id')

result = (
    df.groupby(["cg_id","cg_name"], as_index=False)
      .agg(
          reach=("uniq_imp_id", pd.Series.nunique),   # COUNT DISTINCT
          impressions=("has_won", "sum"),             # SUM(has_won)
          rowcount=("uniq_imp_id", "size")            # COUNT(*)
      )
      .assign(frequency=lambda x: x["rowcount"] / x["reach"])
      .drop(columns="rowcount")
      .reset_index()
      .sort_values("frequency", ascending=False)
)

user_freq = (
    df.groupby(["cg_id", "cg_name", "uniq_imp_id"])
      .agg(imps=("has_won", "sum"))   # impressions per user
      .reset_index()
)

freq_dist = (
    user_freq.groupby(["cg_id", "cg_name", "imps"])
             .size()
             .reset_index(name="users")
             .sort_values(["cg_id", "imps"])
)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None) 
#pd.set_option('display.max_rows', None)

# Frequency tables by fixed windows (pandas-only)
import pandas as pd
import numpy as np
from typing import List, Optional
from datetime import timedelta

# ---- CONFIG ----
TIME_COL = "local_date"      # change if your timestamp column is named differently
IMP_COL  = "has_won"         # 1 per delivered impression row (or integer count if pre-aggregated)

GROUP_COLS = [c for c in ["cg_id", "cg_name"] if c in df.columns]

# ---- Sanity checks / dtype cleanup (idempotent) ----
missing = [col for col in [TIME_COL, "cg_id", "uniq_imp_id", IMP_COL] if col not in df.columns]
if missing:
    raise KeyError(f"df is missing required columns: {missing}")

df = df.copy()
df[IMP_COL] = pd.to_numeric(df[IMP_COL], errors="coerce").fillna(0).astype(int)
df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")

# Core frequency builders
def freq_distribution(
    data: pd.DataFrame,
    group_cols: List[str],
    time_col: Optional[str],
    end_time: Optional[pd.Timestamp],
    window_days: Optional[int],
    thresholds: List[int],
    cumulative: bool = True,
    imp_col: str = "has_won",
) -> pd.DataFrame:
    """
    Build campaign-level frequency table.
      - cumulative=True  -> N+ buckets (Google-style)
      - cumulative=False -> exact-N buckets (users who saw exactly N times)
    """
    d = data
    if end_time is None:
        end_time = d[time_col].max() if time_col else None
    if window_days is not None:
        if time_col is None:
            raise ValueError("time_col required when window_days is not None")
        start = end_time - timedelta(days=window_days)
        d = d[(d[time_col] >= start) & (d[time_col] < end_time)]
    # impressions per user within the window (per campaign)
    user_freq = (
        d.groupby(group_cols + ["uniq_imp_id"], as_index=False)[imp_col]
         .sum()
         .rename(columns={imp_col: "imps"})
    )
    # base campaign metrics
    base = (
        user_freq.groupby(group_cols, as_index=False)
                 .agg(reach=("uniq_imp_id", "nunique"),
                      impressions=("imps", "sum"))
    )
    base["avg_imp_per_user"] = base["impressions"] / base["reach"]
    out = base
    if cumulative:
        # N+ buckets
        for k in thresholds:
            lbl = f"{k}+"
            kplus = (user_freq.loc[user_freq["imps"] >= k]
                     .groupby(group_cols)["uniq_imp_id"].nunique()
                     .rename(lbl)
                     .reset_index())
            out = out.merge(kplus, on=group_cols, how="left")
        bucket_cols = [f"{k}+" for k in thresholds]
    else:
        # exact-N buckets
        exact = (user_freq.groupby(group_cols + ["imps"])["uniq_imp_id"]
                           .nunique()
                           .rename("users")
                           .reset_index())
        exact_wide = (exact.pivot_table(index=group_cols, columns="imps", values="users", fill_value=0)
                           .rename(columns=lambda n: f"{int(n)}x")
                           .reset_index())
        out = out.merge(exact_wide, on=group_cols, how="left")
        bucket_cols = [c for c in out.columns if c.endswith("x")]
    # fill missing buckets and tidy
    if bucket_cols:
        out[bucket_cols] = out[bucket_cols].fillna(0).astype(int)
    ordered = group_cols + ["reach", "avg_imp_per_user", "impressions"] + bucket_cols
    out = out[ordered].sort_values(["avg_imp_per_user"], ascending=False)
    return out


def freq_distribution_range(
    data: pd.DataFrame,
    group_cols: List[str],
    time_col: str,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    thresholds: List[int],
    cumulative: bool = True,
    imp_col: str = "has_won",
    label: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build frequency table for a fixed [start_date, end_date) window (half-open).
    """
    d = data.copy()
    d[time_col] = pd.to_datetime(d[time_col])
    start_date = pd.to_datetime(start_date)
    end_date   = pd.to_datetime(end_date)
    d = d[(d[time_col] >= start_date) & (d[time_col] < end_date)]
    out = freq_distribution(
        data=d,
        group_cols=group_cols,
        time_col=None,     # no rolling window inside: already filtered by explicit range
        end_time=None,
        window_days=None,
        thresholds=thresholds,
        cumulative=cumulative,
        imp_col=imp_col
    )
    if label:
        out.insert(len(group_cols), "window", label)
    return out

# Weekly buckets:
windows = [
    ("2025-07-30", "2025-08-06", "Weekly (7/30-8/5)"),
    ("2025-08-06", "2025-08-13", "Weekly (8/6-8/12)"),
    ("2025-08-13", "2025-08-20", "Weekly (8/13-8/19)"),
    ("2025-08-20", "2025-08-27", "Weekly (8/20-8/26)"),
    ("2025-08-27", "2025-09-03", "Weekly (8/27-9/2)"),
    ("2025-09-03", "2025-09-06", "Weekly (9/3-9/5)"),
]

# Total flight:
total_window = ("2025-07-30", "2025-08-20", "Total (7/30-8/19)")

total_window2 = ("2025-07-30", "2025-09-06", "Total (7/30-9/05)")

# Frequency thresholds:
# - Weekly: 1+..20+ cumulative
weekly_thresholds = list(range(1, 51))
# - Total: you can keep 1+..20+ as well (or customize)
total_thresholds  = list(range(1, 51))

# Build all tables
all_tbls = []
for start, end, lbl in windows:
    t = freq_distribution_range(
        data=df,
        group_cols=GROUP_COLS,
        time_col=TIME_COL,
        start_date=start,
        end_date=end,
        thresholds=weekly_thresholds,
        cumulative=True,
        imp_col=IMP_COL,
        label=lbl
    )
    all_tbls.append(t)

# Total flight
start, end, lbl = total_window
t_total = freq_distribution_range(
    data=df,
    group_cols=GROUP_COLS,
    time_col=TIME_COL,
    start_date=start,
    end_date=end,
    thresholds=total_thresholds,
    cumulative=True,
    imp_col=IMP_COL,
    label=lbl
)
all_tbls.append(t_total)
start, end, lbl = total_window2
t_total2 = freq_distribution_range(
    data=df,
    group_cols=GROUP_COLS,
    time_col=TIME_COL,
    start_date=start,
    end_date=end,
    thresholds=total_thresholds,
    cumulative=True,
    imp_col=IMP_COL,
    label=lbl
)
all_tbls.append(t_total2)
# Sum of impressions where campaign_id == 123
df.loc[df["cg_id"] == 373706, "has_won"].sum()
df.loc[df["cg_id"] == 373706, "local_date"].max()
df.loc[df["cg_id"] == 373706, "local_date"].min()

# Combined final table
final_freq = pd.concat(all_tbls, ignore_index=True)

# Optional: choose a concise view to inspect quickly
preview_cols = GROUP_COLS + ["window", "reach", "avg_imp_per_user", "impressions", "1+", "5+", "10+", "20+"]
existing_preview_cols = [c for c in preview_cols if c in final_freq.columns]
print(final_freq[existing_preview_cols].head(20))

output_path = '/Users/abdullah.arshad/Desktop/griff_test2.csv'
final_freq.to_csv(output_path, index=False, encoding="utf-8-sig")
