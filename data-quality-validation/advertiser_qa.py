import pandas as pd
from io import StringIO
import re
import sys
import os
import platform
from snowflake.snowpark import Session
from datetime import datetime, timedelta

sysname = platform.uname()[0]
#Import keys from our master key folder
if platform.system() == 'Linux':
    print("Server")
    credential_path='/home/ec2-user/pa-lead/nomad/Common_Artifacts_Python'
    sys.path.append(os.path.abspath(credential_path))
    from pyKeys import *
    from Common_Functions import *
    from functions import *

def slack_connect():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    client = WebClient(token=slack_token)
    return client

#Post localized status to our team's slack alert channel
pa_team_bot_channel = "C03N6995W7P"
slack_client = slack_connect()
slack_client.chat_postMessage(channel = pa_team_bot_channel, text = ":test_tube: :test_tube: Running Advertiser check")

time_start = datetime.now()
for y in range(1,2): # We are checking for yesterday's records only.
    # Define yesterday's date for the query
    date_check = (datetime.now() - timedelta(days=y)).strftime('%Y_%m_%d') # Date For Redshift format IN UTC
    date_check_full = (datetime.now() - timedelta(days=y)).strftime('%Y-%m-%d') # Date For Mysql format IN UTC
    # Query to pull advertiser_id and spend for highest spending accounts for `yesterday`
    query_daily = f"""select advertiser_id, 
    SUM(cost) / 1000000.0 AS RS_cost,
    sum(has_won) as RS_imps,
    SUM(has_click) as RS_clicks,
    SUM(has_conv) as RS_conv,
    SUM(has_ltconv) as RS_lt_conv
    from sa_rs_evt_table_{date_check} group by 1;
    """
    # Query each cluster
    clusters_data = []
    for i in range(4): # Iterate across the 4 distinct production database clusters to collect localized logs
        engine = RS_Daily_conn_func(i)
        df = pd.read_sql(query_daily, engine)
        df['Cluster'] = str(i)
        clusters_data.append(df)
        engine.dispose()
    clusters = pd.concat(clusters_data, ignore_index=True) #concat all cluster's chunks together
    # Check discrepancies with STATS_TIDB
    for index, row in clusters.iterrows():
        temp_adv = row['advertiser_id']
        stats_engine = choose_db('STATS_TIDB')
        query_stats = f"""
        SELECT DATE(cached_hour) as date, 
        SUM(cost) / 1000000.0 as cost,
        SUM(imp) as imps,
        SUM(click) as stats_click,
        SUM(conv) as stats_conv,
        SUM(ltc) as stats_lt_conv,
        SUM(skan_conv) as skans
        FROM sa_stats.campaign_daily_stats
        WHERE user_id = {temp_adv}
          AND DATE(cached_hour) >= '{date_check_full} 00:00:00'
          AND DATE(cached_hour) < '{date_check_full} 23:59:59'
        GROUP BY DATE(cached_hour)
        ORDER BY DATE(cached_hour);
        """
        stats_data = pd.read_sql(query_stats, stats_engine)
        if not stats_data.empty:
            clusters.at[index, 'Stat_cost'] = stats_data['cost'].sum()
            clusters.at[index, 'Stat_imps'] = stats_data['imps'].sum()
            clusters.at[index, 'stats_click'] = stats_data['stats_click'].sum()
            clusters.at[index, 'stats_conv'] = stats_data['stats_conv'].sum()
            clusters.at[index, 'stats_lt_conv'] = stats_data['stats_lt_conv'].sum()
            clusters.at[index, 'skans'] = stats_data['skans'].sum()
            # Subtract skans from stats_conv and stats_lt_conv if skans > 0. This is because it is missing in the RS logs.
            if pd.notnull(clusters.at[index, 'skans']) and clusters.at[index, 'skans'] > 0:
                clusters.at[index, 'stats_conv'] = max(
                    0, clusters.at[index, 'stats_conv'] - clusters.at[index, 'skans']
                )
                clusters.at[index, 'stats_lt_conv'] = max(
                    0, clusters.at[index, 'stats_lt_conv'] - clusters.at[index, 'skans']
                )
        stats_engine.dispose()
    clusters = clusters.drop(columns=['skans'])
    # Calculate difference and percentage discrepancy
    clusters['cost_diff_tidb'] = round(clusters['Stat_cost'] - clusters['rs_cost'], 4)
    clusters['cost_perc_tidb'] = round((clusters['cost_diff_tidb'] / clusters['rs_cost']) * 100, 4)
    clusters['imps_diff_tidb'] = round(clusters['Stat_imps'] - clusters['rs_imps'], 4)
    clusters['imps_perc_tidb'] = round((clusters['imps_diff_tidb'] / clusters['rs_imps']) * 100, 4)
    clusters['click_diff_tidb'] = round(clusters['stats_click'] - clusters['rs_clicks'], 4)
    clusters['click_perc_tidb'] = round((clusters['click_diff_tidb'] / clusters['rs_clicks']) * 100, 4)
    clusters['conv_diff_tidb'] = round(clusters['stats_conv'] - clusters['rs_conv'], 4)
    clusters['conv_perc_tidb'] = round((clusters['conv_diff_tidb'] / clusters['rs_conv']) * 100, 4)
    clusters['ltconv_diff_tidb'] = round(clusters['stats_lt_conv'] - clusters['rs_lt_conv'], 4)
    clusters['ltconv_perc_tidb'] = round((clusters['ltconv_diff_tidb'] / clusters['rs_lt_conv']) * 100, 4)
    # Set flags based on discrepancy. 0.5% is the allowable standard management set for us. Abs difference as we dont care if its under/over reporting. Highlighting disc is more important. 
    clusters['automation_flag'] = (
    (clusters['cost_perc_tidb'].abs() > 0.5) |
    (clusters['imps_perc_tidb'].abs() > 0.5) |
    (clusters['click_perc_tidb'].abs() > 0.5) |
    (clusters['conv_perc_tidb'].abs() > 0.5) |
    (clusters['ltconv_perc_tidb'].abs() > 0.5))
    automation_flag = clusters['automation_flag'].any()
    clusters['date']=date_check_full
    # Output results
    print("Automation flag:", automation_flag)
    if automation_flag:
        print("Discrepancies found:")
        print(clusters[clusters['automation_flag']])
        slack_client.chat_postMessage(channel = pa_team_bot_channel, text = f"QA Check: Discrepencies seen in {len(clusters[clusters['automation_flag']])} advertisers")
        slack_client.chat_postMessage(channel = pa_team_bot_channel, text = f"Hey <@U0247H214CS>, <!subteam^S08AEE05B4Z> tagging to let you know!")
    print(f"Max perc discrepancy (cost): {clusters['cost_perc_tidb'].abs().max()}")
    print(f"Max perc discrepancy (clicks): {clusters['click_diff_tidb'].abs().max()}")
    print(f"Max perc discrepancy (convs): {clusters['conv_diff_tidb'].abs().max()}")
    print(f"Max perc discrepancy (lt_convs): {clusters['ltconv_diff_tidb'].abs().max()}")
    time_end = datetime.now()
    print("Script runtime:", time_end - time_start)
    clusteradv_list = clusters.loc[clusters['automation_flag'] == False, 'advertiser_id']
    def snowflake_session():
        def snowpark_session_create():
            connection_params = {
                "account": pa_sf_odbc,
                "user": pa_sf_uid,
                "password": pa_sf_pwd,
                "role": "ACCOUNTADMIN",
                "warehouse": pa_sf_warehouse
            }
            session = Session.builder.configs(connection_params).create()
            session.sql_simplifier_enabled = True
            return session
        session = snowpark_session_create()  
        return session
    session = snowflake_session()
    session.use_database("PA_INTERNAL")
    session.use_schema("PUBLIC")
    # Check and delete rows for today if already pulled.
    session.sql(f"""
        DELETE FROM "adv_list"
        WHERE "date" = '{date_check_full}';
    """).collect()
    session.write_pandas(
        clusters,
        table_name="adv_list", 
        auto_create_table=True,overwrite=False
    )
