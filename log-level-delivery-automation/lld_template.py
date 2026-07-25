#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 16:41:05 2026

@author: abdullah.arshad
"""
import io
import os
import sys
import platform
from datetime import date, datetime, timedelta
import boto3
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
SERVICE_ACCOUNT_FILE = "/home/ec2-user/python_auth/python_service_account_credential.json"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]
sysname = platform.uname()[0] if hasattr(platform, "uname") else platform.system()
sub_system = platform.uname()[2] if hasattr(platform, "uname") else ""
credentials=None
pull_type = False
sample = False

def slack_getinfo(client):
    # Gets all users on slack
    users = []
    for paginated_response in client.users_list(
        limit=1000
    ):  # this is required in order to return more than 1,000 rows
        users += paginated_response["members"]
    users = pd.DataFrame(users)
    users = users.drop(
        users[users.deleted == True].index
    )  # Drops rows where user is no longer at the company
    profiles = users["profile"]
    profile_list = list(
        [
            d
            for d in profiles
            if re.search(r"Data Architect", d.get("title", ""))
            or re.search(r"Scaled Analytics", d.get("title", ""))
            or re.search(r"Reporting Architect", d.get("title", ""))
            or re.search(r"Client Analytics", d.get("title", ""))
        ]
    )
    profiles_realname = [
        p_name for i in profile_list if (p_name := i.get("real_name")) is not None
    ]
    # List of PAs
    pa_team = pd.DataFrame(users.loc[users["real_name"].isin(profiles_realname)])
    # pd.set_option('display.max_columns', None)
    pa_team = pa_team[["id", "real_name"]]
    def extract_first_word(text):
        return text.split()[0]
    pa_team["first_name"] = pa_team["real_name"].apply(extract_first_word)
    return pa_team

def env_to_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"true", "1", "yes", "y", "t"}

def parse_column_renames(rename_str: str) -> dict:
    """
    Converts 'old1:new1,old2:new2' → {'old1': 'new1', 'old2': 'new2'}
    """
    mapping = {}
    if not rename_str:
        return mapping
    pairs = [p.strip() for p in rename_str.split(",") if p.strip()]
    for pair in pairs:
        if ":" not in pair:
            print(f"Skipping invalid rename pair: {pair}")
            continue
        old, new = pair.split(":", 1)
        mapping[old.strip()] = new.strip()
    return mapping


# Following codeblock checks to see if we are running on Kestra or Linux machine.
# Note, because we need to touch our internal s3, we cannot run this on our local machines. 
if platform.system() == "Linux":
    if sub_system == "6.1.49-69.116.amzn2023.x86_64":
        sys.path.append(os.path.abspath("/home/ec2-user/pa-lead/python_schedule/"))
        sys.path.append(os.path.abspath("/home/ec2-user/pa-lead/"))
        from functions import *
        from Query_Builder import QueryBuilder
        from Common_Functions import *
        slack_token = os.getenv("SLACK_TOKEN", "")
        if not slack_token and "slack_token" in globals():
            slack_token = globals().get("slack_token", "")
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=SCOPES,
        )
    else:
        print("Server (kestra path)") 
        credential_path = "kestra/Common_Artifacts_Python"
        sys.path.append(os.path.abspath(credential_path))
        from pyKeys import *
        from Common_Functions import *
        from functions import *
        credentials = service_account.Credentials.from_service_account_info(
            google_services_creds_json,
            scopes=SCOPES,
        )
        pull_type = True
# For viewability in code
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

if pull_type:
    print("Productized LLD Export or Kestra flow execution")
    fs_ticket_id = int(os.getenv("fs_ticket_id"))
    account_id = int(os.getenv("account_id"))
    filter_level = os.getenv("filter_level")
    filter_value = os.getenv("filter_value")
    start_date = os.getenv("start_date")
    end_date = os.getenv("end_date")
    frequency = os.getenv("frequency")
    requested_columns_raw = os.getenv("requested_columns", "")
    final_destination = os.getenv("final_destination", "").strip().lower()
    final_destination_var1 = os.getenv("final_destination_var1", "").strip()
    final_destination_var2 = os.getenv("final_destination_var2", "").strip()
    requested_columns = [
        col.strip() for col in requested_columns_raw.split(",") if col.strip()
    ]
    requested_column_renames_raw = os.getenv("requested_column_renames", "").strip()
    custom_case_statements = os.getenv("custom_case_statements", "").strip()
    owner = os.getenv("OWNER_NAME", "").strip()
    custom_case_statements=""
    repull = env_to_bool("repull", default=False)
    sample = env_to_bool("sample", default=False)
else:
    print("EC2-Instance LLD") # this was used to testing, should not be triggered anymore. 
    fs_ticket_id = 12345
    account_id = 37457
    filter_level = "campaign_id"
    filter_value = "3075490,3075527,3179963"
    start_date = "2026-04-01"
    end_date = "2026-05-31"
    frequency = "daily"
    requested_columns = [
        "request_time",
        "campaign_id",
        "matched_cseg_and_rt",
        "network_id",
        "conv_tracker_id",
        "nativead_id",
        "has_won",
        "cost",
    ]
    final_destination = "snowflake"
    final_destination_var1 = ""
    final_destination_var2 = ""
    sample = False #If you want sample to be uploaded ONLY
    owner='Abdullah'

initial_pull_required = True

# Get correct timezone for the account
mydb = mydb_conn_func()
user_timezon_nativeads = pd.read_sql(f"SELECT timezone from users where id in ({account_id})",con = mydb)
internal_analyticsdb_conn = internal_analyticsdb_conn_func()
user_timezone_df = pd.read_sql(f"SELECT RS_timezone FROM 11_utc_adjust WHERE timezone = '{user_timezon_nativeads['timezone'].max()}'",con = internal_analyticsdb_conn)
user_timezone = user_timezone_df['RS_timezone'].max()

# Potential inputs
dimensions = [
    "ad_unit",
    "addon_bitmap",
    "addon_costs",
    "addon_ids",
    "addon_prices",
    "advertiser_categories",
    "advertiser_id",
    "advertiser_domain",
    "all_matched_categories",
    "all_matched_custom_segment",
    "all_matched_retargeting_segment",
    "all_matched_behavioural_segment",
    "all_matched_targeting_cs_data_catalogue",
    "all_matched_cs_data_catalogue",
    "all_matched_custom_behavioural_segment",
    "app_bundle",
    "app_categories",
    "all_matched_shadow_segment",
    "app_content_genre",
    "app_content_language",
    "app_content_length",
    "app_content_channel_name",
    "app_content_live_stream",
    "app_content_rating",
    "app_content_series",
    "app_content_title",
    "app_content_network_name",
    "app_name",
    "app_publisher_id",
    "app_publisher_name",
    "app_info_id",
    "app_info_name",
    "auction_id",
    "app_paid",
    "b2b_company_fortune",
    "b2b_company_revenue",
    "appfigures_categories",
    "applicable_regulation_bitmap",
    "applied_regulation_bitmap",
    "b2b_company_size",
    "audio_banner_dimensions",
    "audio_banner_types",
    "b2b_domain",
    "b2b_functional_area",
    "b2b_industry",
    "b2b_seniority",
    "bid_type",
    "brandname_id",
    "browser",
    "banner_b_attr",
    "banner_b_type",
    "banner_exp_dir",
    "banner_mimes",
    "banner_pos",
    "browser_version",
    "campaign_id",
    "city",
    "bidder_id",
    "cleaned_domain_key",
    "congressional_district",
    "conv_from",
    "bypass_whitelisted_deal_id",
    "campaign_categories",
    "conv_order_id",
    "conv_tracker_id",
    "conv_type",
    "canadian_electoral_district",
    "canadian_provincial_district",
    "conv_url",
    "country",
    "cleaned_genre",
    "click_req_ip",
    "click_req_ip_sha256",
    "county",
    "companion_banner_viewed",
    "creative_id",
    "ctv_publisher_id",
    "deal_id",
    "demo_age_seg",
    "conv_tracker_type",
    "demo_gender_seg",
    "demo_houseincome_seg",
    "cookie_req_ip",
    "cookie_uid_v3_sha256",
    "device_carrier",
    "device_connection",
    "device_dnt",
    "device_geo_lat",
    "creative_height",
    "device_geo_long",
    "creative_width",
    "device_id",
    "device_id_md5",
    "device_id_sha",
    "device_id_sha256",
    "device_ifa",
    "device_isp",
    "device_language",
    "device_make",
    "device_model",
    "device_os_vsersion",
    "device_type",
    "dma",
    "domain_key",
    "duration",
    "footfall_tracker_id",
    "full_zipcode",
    "has_conversion_tracker",
    "has_engagement_tracker",
    "has_session",
    "hashed_ip",
    "height",
    "interaction_level",
    "interaction_value",
    "ip_address",
    "email_advertiser_merge_tags",
    "email_bounce_type",
    "email_campaign_content_id",
    "email_cdp_merge_tags",
    "email_dco_merge_tags",
    "email_event_timestamp",
    "email_event_type",
    "email_first_event_occurrence",
    "email_id",
    "email_recipient_address",
    "email_reference_id",
    "email_send_template_id",
    "email_sent_timestamp",
    "email_unsubscribe_type",
    "email_variant_id",
    "email_workflow_id",
    "email_workflow_instance_id",
    "email_workflow_step_id",
    "ip_address_sha256",
    "event_type",
    "experimental_group_type",
    "ip_type_bit_value",
    "is_ctv",
    "geo_polygon_id",
    "gpid",
    "is_desktop",
    "is_dooh",
    "is_inapp",
    "is_mobile",
    "is_ott",
    "is_tablet",
    "last_ip",
    "line_item_id",
    "liveramp_id",
    "local_day_of_week",
    "local_hour_of_day",
    "has_viewability_tracker",
    "local_visit_timestamp",
    "location_id",
    "lrid",
    "house_district",
    "imp_display_manager",
    "imp_display_manager_ver",
    "imp_index",
    "imp_instl",
    "lt_conv_from",
    "imp_tag_id",
    "lt_conv_type",
    "lt_s_conv_from",
    "lt_s_conv_type",
    "matched_behavioural_segment",
    "matched_cseg_and_rt",
    "matched_cseg_and_rt_type",
    "is_cellular",
    "matched_inventory_package_id",
    "matched_segment_source_id",
    "matched_venue_type_id",
    "is_guaranteed",
    "most_associated_duid",
    "is_ip_generated",
    "most_associated_duid_hashed",
    "most_associated_duid_sha256",
    "is_private",
    "is_promised",
    "is_self",
    "most_associated_duid_tier",
    "jounce_primary_seller_name",
    "jounce_seller_parent_name",
    "most_associated_ip",
    "lat_id_sha256",
    "layout",
    "most_associated_ip_hashed",
    "nativead_format",
    "nativead_id",
    "network_id",
    "os_family",
    "page",
    "liveramp_id_sha256",
    "page_categories",
    "property_id",
    "property_targeting_type",
    "received",
    "region",
    "lrid_hashed",
    "lrid_sha256",
    "lrid_source",
    "req_uniq_id",
    "request_demo_segments",
    "request_duid",
    "request_duid_hashed",
    "ltconv_camp_ids",
    "ltconv_end_time",
    "request_duid_sha256",
    "ltconv_start_time",
    "request_id",
    "request_inventory_subtype",
    "request_time",
    "request_user_id",
    "matched_cseg_and_rt_detail",
    "s_conv_from",
    "matched_custom_behavioural_segment",
    "matched_deal_group_ids",
    "s_conv_type",
    "matched_rule_group_id",
    "site_content_genre",
    "matched_tactic_bid_ids",
    "matched_tactic_native_ad_ids",
    "site_content_language",
    "site_content_length",
    "site_content_live_stream",
    "site_content_rating",
    "site_content_series",
    "site_content_title",
    "site_id",
    "most_associated_ip_sha256",
    "site_publisher_id",
    "site_publisher_name",
    "site_ref",
    "opt_out_decision_bitmap",
    "opt_out_decision_timestamp_milli",
    "optimization_key",
    "sub_advertiser_id",
    "sub_domain",
    "supply_inventory_type",
    "postback_conv_val",
    "profile_age",
    "timestamp",
    "uniq_imp_id",
    "uniq_imp_id_sha256",
    "user_agent",
    "reg",
    "user_buyer_uid",
    "video_placement",
    "video_skip",
    "video_type",
    "width",
    "zipcode",
    "request_duid_tier",
    "request_inventory_type",
    "sa_region",
    "schain_asi",
    "schain_complete",
    "schain_sid",
    "schain_size",
    "senate_district",
    "site_content_channel_name",
    "site_content_network_name",
    "site_mobile",
    "site_search",
    "sk_source_app_id",
    "source_fd",
    "source_pchain",
    "source_tid",
    "targeting_id",
    "unify_code",
    "user_buyer_uid_sha256",
    "video_api",
    "video_b_attr",
    "video_linearity",
    "video_matched_playback_method",
    "video_matched_player_size",
    "video_max_bitrate",
    "video_max_duration",
    "video_max_view_completion",
    "video_mimes",
    "video_min_bitrate",
    "video_min_duration",
    "video_playback_method",
    "video_pos",
    "video_protocols",
    "video_start_delay",
    "view_depth",
    "win_notice_ip",
    "win_notice_ip_sha256"
]
metrics = [
    "addon_cost",
    "addon_price",
    "arrival_rate_score",
    "bid_floor",
    "bid_price_prered",
    "conversion_optimization_weight",
    "email_estimated_price",
    "en_ad_inc",
    "en_ad_sa_a",
    "en_ad_sa_c",
    "en_ad_sa_d",
    "en_ad_sa_n",
    "en_ad_sa_v",
    "en_ad_tp_a",
    "en_ad_tp_c",
    "en_ad_tp_d",
    "en_ad_tp_n",
    "en_ad_tp_v",
    "en_adv_b2b",
    "en_adv_brow",
    "en_adv_kwr",
    "en_adv_lal",
    "en_adv_lalx",
    "en_adv_pcai",
    "en_aud_xdevice",
    "en_eng",
    "en_ga",
    "en_meas_bl",
    "en_meas_dco",
    "en_meas_fpda",
    "en_meas_frd_a",
    "en_meas_frd_c",
    "en_meas_frd_d",
    "en_meas_frd_n",
    "en_meas_frd_v",
    "en_meas_fta",
    "en_meas_hc",
    "en_meas_inc_rc",
    "en_meas_saf_a",
    "en_meas_saf_c",
    "en_meas_saf_d",
    "en_meas_saf_n",
    "en_meas_saf_v",
    "en_meas_view_a",
    "en_meas_view_c",
    "en_meas_view_d",
    "en_meas_view_n",
    "en_meas_view_v",
    "en_ml_baseline",
    "en_ml_opt",
    "en_mng_marg",
    "en_perf_goal",
    "en_pl_marg_pg",
    "en_pl_margin",
    "en_std_appfig",
    "en_std_crm",
    "en_std_demo",
    "en_std_device",
    "en_std_drt",
    "en_std_georad",
    "en_std_int",
    "en_std_ip",
    "en_std_isp",
    "en_std_lang",
    "en_std_rt",
    "en_std_tac_w",
    "en_tp",
    "has_engage_wall",
    "has_key_phrase",
    "page_score",
    "pred_score",
    "projected_lt_footfall_conv",
    "promised_cost",
    "advertiser_conversion_revenue_deduped",
    "advertiser_has_conversion_deduped",
    "advertiser_has_secondary_conversion_deduped",
    "bid_price",
    "bid_reduction",
    "campaign_conversion_revenue",
    "campaign_pacing",
    "click_time_diff",
    "cost",
    "cost_usd",
    "en_std_cat",
    "has_click",
    "has_conv",
    "has_engagement",
    "has_footfall_conv",
    "has_lt_footfall_conv",
    "has_ltconv",
    "stats_50v_2s_ias",
    "has_s_ltconv",
    "has_secondary_conversion",
    "has_won",
    "imp_multiplier",
    "imp_time_diff",
    "line_item_conversion_revenue_deduped",
    "stats_click_time",
    "stats_cpm_cost",
    "stats_engage_click",
    "stats_engage_cost",
    "stats_engage_cpm",
    "stats_engage_imp",
    "stats_fraud_ias",
    "stats_imp_time",
    "stats_measure_ias",
    "line_item_has_conversion_deduped",
    "line_item_has_secondary_conversion_deduped",
    "stats_moat_non_measure",
    "stats_page_time_120s",
    "line_item_unique_imp",
    "ltconv_revenue",
    "stats_page_time_300s",
    "stats_page_time_30s",
    "stats_page_time_5s",
    "stats_page_time_60s",
    "stats_page_time_900s",
    "stats_page_view",
    "margin",
    "margin_adjusted",
    "projected_footfall_conv",
    "stats_acomp_0",
    "stats_acomp_25",
    "stats_acomp_50",
    "stats_acomp_75",
    "stats_acomp_95",
    "stats_b_cost",
    "stats_moat_inview",
    "stats_moat_measure",
    "stats_page_time_15s",
    "stats_page_time_1s",
    "stats_profit",
    "stats_revenue",
    "stats_ssp_revenue",
    "stats_tp_cpc_cost",
    "stats_tp_cpm_cost",
    "stats_vcomp_0",
    "usd_to_local_exchange_rate",
    "stats_vcomp_25",
    "stats_vcomp_50",
    "stats_vcomp_75",
    "stats_vcomp_95",
    "sub_advertiser_unique_imp",
    "total_time_on_site",
    "unique_imp",
    "view_50v_2s",
    "view_m",
    "win_bid_time_diff",
    "viewability",
    "win_price_prered",
    "win_price"
]

#Filtered inputs
DERIVED_TIME_COLUMNS = {
    "request_date": lambda tz: f"DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', '{tz}', request_time)) AS request_date",
    "request_hour": lambda tz: f"DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', '{tz}', request_time)) AS request_hour",
}

derived_dims = [d for d in requested_columns if d in DERIVED_TIME_COLUMNS]
dims = [d for d in requested_columns if d in dimensions or d in DERIVED_TIME_COLUMNS]
mets = [d for d in requested_columns if d in metrics]

# Lowest requested hierarchy
hierarchy = [
    "advertiser_id",
    "sub_advertiser_id",
    "line_item_id",
    "campaign_id",
    "nativead_id",
]

#grabbing lowest hier.
lowest_level = next((field for field in reversed(hierarchy) if field in dims), None)
print(f"lowest_level={lowest_level}")

### Check if the folder exists in s3
s3 = boto3.client("s3")
bucket = "stackadapt-pa-team-us-east-1"
base_path = "Internal/LLD/"
prefix = f"{base_path}uid_{account_id}/REQ_{fs_ticket_id}/"

#Checks for folder
def folder_exists(bucket, prefix):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1,
    )
    return "Contents" in response

#checks for file in bucket
def csv_exists_in_prefix(bucket, prefix):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/",  # only look at this level, not subfolders
    )
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key == prefix:
            continue
        if key.endswith(".csv"):
            print(f"Found CSV directly under prefix: {key}")
            return True
    return False

#creates folder
def create_folder(bucket, prefix):
    s3.put_object(Bucket=bucket, Key=prefix)
    print(f"Created folder: {prefix}")

folder_exists_flag = folder_exists(bucket, prefix)

if not folder_exists_flag:
    print("Folder does not exist -> creating folder")
    create_folder(bucket, prefix)
    sample = True
else:
    print("Folder already exists")

file_exists_flag = csv_exists_in_prefix(bucket, prefix)

if file_exists_flag:
    print("CSV file exists -> no initial pull needed")
    initial_pull_required = False
else:
    print("No CSV found under prefix -> initial pull required")
    initial_pull_required = True

# Dates (yesterday / range)
if(initial_pull_required == True or repull == True):
    START_DATE = f"{start_date} 00:00:00"
    END_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59")
    print(f'Pulling data between {start_date} and {(date.today() - timedelta(days=1))}')
else:
    START_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    END_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59")
    print(f'Pulling data for {(date.today() - timedelta(days=1))}')

def get_rs_data(start_date_utc, end_date_utc):
    dur = (
        datetime.strptime(end_date_utc, "%Y-%m-%d %H:%M:%S")
        - datetime.strptime(start_date_utc, "%Y-%m-%d %H:%M:%S")
    ).days + 2
    print(start_date_utc, end_date_utc, dur)
    select_fields = []
    for col in dims + mets:
        if col == "request_time":
            select_fields.append(
                f"CONVERT_TIMEZONE('UTC', '{user_timezone}', request_time) AS request_time"
            )
        elif col in DERIVED_TIME_COLUMNS:
            select_fields.append(DERIVED_TIME_COLUMNS[col](user_timezone))
        else:
            select_fields.append(col)
    if custom_case_statements:
        print("Applying custom CASE statements")
        select_fields.append(custom_case_statements)
    select_clause = ", ".join(select_fields)
    SELECT_stmnt = f"""SELECT
    {select_clause}
    """
    SELECT_stmnt = censor_pii(SELECT_stmnt)
    if filter_level=='nativead_id':
        where_nd = f"WHERE nativead_id IN ({filter_value})"
    elif filter_level== 'campaign_id':
        where_nd = f"WHERE campaign_id IN ({filter_value})"
    elif filter_level== 'line_item_id':
        where_nd = f"WHERE line_item_id IN ({filter_value})"
    elif filter_level== 'sub_advertiser_id':
        where_nd = f"WHERE sub_advertiser_id IN ({filter_value})"
    else:
        where_nd = f"WHERE advertiser_id IN ({account_id})"
    WHERE_stmnt = (
        where_nd
        + " AND request_time >= '"
        + start_date_utc
        + "' AND request_time < '"
        + end_date_utc
        + "'"
    )
    GROUPBY = ""
    ORDERBY = ""
    end_parsed = pd.to_datetime(end_date_utc, format="%Y-%m-%d %H:%M:%S")
    #print(WHERE_stmnt)
    #print(select_fields)
    print(dur, end_parsed, SELECT_stmnt, WHERE_stmnt, GROUPBY, ORDERBY, account_id)
    df = daily_looper_fun(
        dur, end_parsed, SELECT_stmnt, WHERE_stmnt, GROUPBY, ORDERBY, account_id
    )
    if df is None or df.empty:
        return df
    num = mets
    for c in num:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def get_mysql_name_map(filter_val):
    mydb = mydb_conn_func()
    select_fields = [
        "users.id AS account_id",
        "users.company_name AS account_name",
    ]
    if lowest_level == "nativead_id":
        where_nd = f"WHERE native_ads.id IN ({filter_val})"
        select_fields += [
            "sub_advertisers.id AS advertiser_id",
            "sub_advertisers.name AS advertiser_name",
            "line_items.id AS campaign_group_id",
            "line_items.name AS campaign_group_name",
            "campaigns.id AS campaign_id",
            "campaigns.name AS campaign_name",
            "campaigns.type AS campaign_type",
            "native_ads.id AS creative_id",
            "native_ads.name AS creative_name",
        ]
    elif lowest_level == "campaign_id":
        where_nd = f"WHERE campaigns.id IN ({filter_val})"
        select_fields += [
            "sub_advertisers.id AS advertiser_id",
            "sub_advertisers.name AS advertiser_name",
            "line_items.id AS campaign_group_id",
            "line_items.name AS campaign_group_name",
            "campaigns.id AS campaign_id",
            "campaigns.name AS campaign_name",
            "campaigns.type AS campaign_type",
        ]
    elif lowest_level == "line_item_id":
        where_nd = f"WHERE line_items.id IN ({filter_val})"
        select_fields += [
            "sub_advertisers.id AS advertiser_id",
            "sub_advertisers.name AS advertiser_name",
            "line_items.id AS campaign_group_id",
            "line_items.name AS campaign_group_name",
        ]
    elif lowest_level == "sub_advertiser_id":
        where_nd = f"WHERE sub_advertisers.id IN ({filter_val})"
        select_fields += [
            "sub_advertisers.id AS advertiser_id",
            "sub_advertisers.name AS advertiser_name",
        ]
    else:
        where_nd = f"WHERE {filter_level}.id IN ({account_id})"
    q = f"""
    SELECT DISTINCT
        {", ".join(select_fields)}
    FROM native_ads
    INNER JOIN campaigns_native_ads 
        ON campaigns_native_ads.native_ad_id = native_ads.id
    INNER JOIN campaigns
        ON campaigns.id = campaigns_native_ads.campaign_id
    INNER JOIN line_items
        ON campaigns.line_item_id = line_items.id
    INNER JOIN sub_advertisers 
        ON campaigns.sub_advertiser_id = sub_advertisers.id
    INNER JOIN users 
        ON users.id = campaigns.user_id
    {where_nd}
    """
    print(q)
    out = pd.read_sql(q, con=mydb)
    mydb.dispose()
    return out

def get_audience_info(df):
    seg_id_list = df['matched_cseg_and_rt'].unique().astype('str')
    seg_id_list = ','.join(f"'{x}'" for x in seg_id_list)
    mydb = mydb_conn_func()
    #CS
    cs_q = f"SELECT identifier as matched_cseg_and_rt, CASE WHEN display_name IS NULL THEN name ELSE display_name END as Audience FROM custom_segments WHERE identifier IN ({seg_id_list})"
    cs_df = pd.read_sql(cs_q,con = mydb)
    #RT
    rt_q = f"SELECT id as matched_cseg_and_rt, name as Audience FROM rt_segments WHERE id IN ({seg_id_list})"
    rt_df = pd.read_sql(rt_q,con = mydb)
    cs_rt_df = pd.concat([cs_df,rt_df])
    mydb.dispose()
    cs_rt_df['matched_cseg_and_rt'] = cs_rt_df['matched_cseg_and_rt'].astype('str')
    return cs_rt_df

def get_DMA_info():
    ss_data = internal_analyticsdb_conn_func()
    dma_q = f"SELECT DMA_Adjust as dma, DMA_Name as dma_name FROM 16_DMA GROUP BY 1,2;"
    dma_df = pd.read_sql(dma_q, con=ss_data)
    dma_df["dma"] = dma_df["dma"].astype("str")
    ss_data.dispose()
    return dma_df

def deal(df):
    deal_list = df['deal_id'].unique().astype('str')
    deal_list = '","'.join(deal_list)
    mydb = mydb_conn_func()
    deal_q = f'SELECT deal_id , name as deal_name FROM programmatic_deals WHERE deal_id IN ("{deal_list}")'
    deal_df = pd.read_sql(deal_q,con = mydb)
    print(deal_df)
    deal_q = f'select deal_id from nativead_production.inventory_byod_deals where deal_id IN ("{deal_list}")'
    deal_byod = pd.read_sql(deal_q,con = mydb)
    print(deal_byod)
    mydb.dispose()
    deal_df['deal_id'] = deal_df['deal_id'].astype('str')
    deal_byod['deal_id'] = deal_byod['deal_id'].astype('str')
    return deal_df, deal_byod

def get_network_info(df):
    network_id_list = df['network_id'].unique().astype('str')
    network_id_list = ','.join(network_id_list)
    mydb = mydb_conn_func()
    network_q = f"SELECT id as network_id , company_name as network_name FROM ad_networks WHERE id IN ({network_id_list})"
    network_df = pd.read_sql(network_q,con = mydb)
    mydb.dispose()
    network_df['network_id'] = network_df['network_id'].astype('str')
    return network_df

def getconv(df):
    getconv_list = df['conv_tracker_id']
    getconv_list = getconv_list[getconv_list.notna() & (getconv_list != '')].unique()
    getconv_list = ','.join(getconv_list.astype(str))
    mydb = mydb_conn_func()
    conv_q = f"SELECT id as conv_tracker_id , name as conversion_name FROM conversion_trackers WHERE id IN ({getconv_list})"
    conv_df = pd.read_sql(conv_q,con = mydb)
    mydb.dispose()
    conv_df['conv_tracker_id'] = conv_df['conv_tracker_id'].astype('str')
    return conv_df

def build_report(df,lowest_level_filter):
    if df is None or df.empty:
        return df
    #Mapp certain fields.
    # Skip entity-name enrichment if no hierarchy field was requested
    if lowest_level is None:
        print("No hierarchy field requested; skipping entity-name enrichment.")
    else:
        mapping = get_mysql_name_map(lowest_level_filter)
        if lowest_level == 'nativead_id':
            df = df.merge(mapping, how='left', left_on='nativead_id', right_on='creative_id')
        elif lowest_level == 'campaign_id':
            df = df.merge(mapping, how='left', on='campaign_id')
        elif lowest_level == 'line_item_id':
            df = df.merge(mapping, how='left', left_on='line_item_id', right_on='campaign_group_id')
        elif lowest_level == 'sub_advertiser_id':
            df = df.merge(mapping, how='left', left_on='sub_advertiser_id', right_on='advertiser_id')
        elif lowest_level == 'advertiser_id':
            df = df.merge(mapping, how='left', left_on='advertiser_id', right_on='account_id')
    if "matched_cseg_and_rt" in dims:
        cseg_tbl = get_audience_info(df)
        df = df.merge(cseg_tbl,how ='left',on = 'matched_cseg_and_rt')
    if "dma" in dims:
        dma_tbl = get_DMA_info()
        df['dma'] = df['dma'].astype('str')
        df = df.merge(dma_tbl,how ='left',on = 'dma')
    if "conv_tracker_id" in dims:
        if df['conv_tracker_id'].fillna('').ne('').any():
            conv_names = getconv(df)
            df['conv_tracker_id'] = df['conv_tracker_id'].astype('str')
            conv_names['conv_tracker_id'] = conv_names['conv_tracker_id'].astype('str')
            df = df.merge(conv_names,how ='left',on = 'conv_tracker_id')
    if "network_id" in dims:
        network_map_tbl = get_network_info(df)
        df["network_id"] = df["network_id"].astype("str")
        df = df.merge(network_map_tbl, how="left", on="network_id")
    if "deal_id" in dims:
        deal_df, deal_byod = deal(df)
        df["deal_id"] = df["deal_id"].fillna("").astype(str)
        df = df.merge(deal_df, how="left", on="deal_id")
        # Keep deal_id only if in BYOD
        byod_ids = set(deal_byod["deal_id"].astype(str))
        df["deal_id"] = df["deal_id"].where(df["deal_id"].isin(byod_ids), "")
    print(df)
    #Removing all dup ids
    df = df.drop(columns=[col for col in df.columns if col.endswith('_y')])
    df = df.rename(columns={col: col[:-2] for col in df.columns if col.endswith('_x')})
    
    #Dividing some metrics by 1000000.0
    if "cost" in mets:
        df['cost'] = pd.to_numeric(df['cost'], errors='coerce') / 1000000.0
    if "line_item_conversion_revenue_deduped" in mets:
        df['line_item_conversion_revenue_deduped'] = pd.to_numeric(df['line_item_conversion_revenue_deduped'], errors='coerce') / 1000000.0
    if "advertiser_conversion_revenue_deduped" in mets:
        df['advertiser_conversion_revenue_deduped'] = pd.to_numeric(df['advertiser_conversion_revenue_deduped'], errors='coerce') / 1000000.0
    if "campaign_conversion_revenue" in mets:
        df['campaign_conversion_revenue'] = pd.to_numeric(df['campaign_conversion_revenue'], errors='coerce') / 1000000.0
    if "cost_usd" in mets:
        df['cost_usd'] = pd.to_numeric(df['cost_usd'], errors='coerce') / 1000000.0
    if "stats_revenue" in mets:
        df['stats_revenue'] = pd.to_numeric(df['stats_revenue'], errors='coerce') / 1000000.0
    if "stats_profit" in mets:
        df['stats_profit'] = pd.to_numeric(df['stats_profit'], errors='coerce') / 1000000.0
    if "ltconv_revenue" in mets:
        df['ltconv_revenue'] = pd.to_numeric(df['ltconv_revenue'], errors='coerce') / 1000000.0
    if "stats_tp_cpm_cost" in mets:
        df['stats_tp_cpm_cost'] = pd.to_numeric(df['stats_tp_cpm_cost'], errors='coerce') / 1000000.0
    if "stats_tp_cpc_cost" in mets:
        df['stats_tp_cpc_cost'] = pd.to_numeric(df['stats_tp_cpc_cost'], errors='coerce') / 1000000.0
    if "en_pl_margin" in mets:
        df['en_pl_margin'] = pd.to_numeric(df['en_pl_margin'], errors='coerce') / 1000000.0
    
    return df

print(f"Pull: {START_DATE} → {END_DATE} ({user_timezone})")
s_utc, e_utc = utc_translation(START_DATE, END_DATE, user_timezone)
df_rs = get_rs_data(s_utc, e_utc)

if df_rs is None or df_rs.empty:
    print("No data returned for requested window.")
    sys.exit(0)

if lowest_level == "advertiser_id":
    lowest_level_filter = str(account_id)
elif lowest_level is not None:
    lowest_level_filter = ','.join(
        f"'{x}'" for x in df_rs[lowest_level].dropna().unique()
    )
else:
    lowest_level_filter = None

#mapping = get_mysql_name_map(lowest_level_filter)
#print(lowest_level)

report = build_report(df_rs,lowest_level_filter)
print(report)

rename_map = parse_column_renames(requested_column_renames_raw)

if rename_map:
    print(f"Applying column renames: {rename_map}")
    valid_renames = {k: v for k, v in rename_map.items() if k in report.columns}
    missing = set(rename_map.keys()) - set(valid_renames.keys())
    if missing:
        print(f"Warning: These columns were not found for renaming: {missing}")
    report = report.rename(columns=valid_renames)
# ### ### ###
# Report Ready!
# ### ### ###

# Write to our personal s3 ALWAYS
start_label = pd.to_datetime(START_DATE).strftime("%Y-%m-%d")
end_label = pd.to_datetime(END_DATE).strftime("%Y-%m-%d")

#if this is a sample
if sample:
    print('we are sampling here.')
    report = report.head(20)
    prefix = f'{prefix}sample/'

filename = f"Stackadapt_logfile_{start_label}_{end_label}.csv"
s3_key = f"{prefix}{filename}"
internal_s3=s3_key
csv_buffer = io.StringIO()
report.to_csv(csv_buffer, index=False)

#Write to our S3
response = s3.put_object(
    Bucket=bucket,
    Key=s3_key,
    Body=csv_buffer.getvalue()
)

print("put_object response:", response)

try:
    head = s3.head_object(Bucket=bucket, Key=s3_key)
    print("head_object succeeded")
    print("ContentLength:", head["ContentLength"])
    print("LastModified:", head["LastModified"])
except Exception as e:
    print("head_object failed:", repr(e))

list_resp = s3.list_objects_v2(Bucket=bucket, Prefix=s3_key)
print("list_objects_v2 for exact key:", list_resp.get("Contents", []))
print(f"Uploaded to s3://{bucket}/{s3_key}")

## DELIVER TO CLIENT ###
import re
def normalize_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    print("Normalizing date fields")
    out = df.copy()
    requested_time_columns = {
        "request_time",
        "request_date",
        "request_hour",
        "timestamp",
        "ltconv_start_time",
        "ltconv_end_time",
        "email_event_timestamp",
        "email_sent_timestamp",
        "local_visit_timestamp",
        "stats_click_time",
        "stats_imp_time",
    }
    def is_time_like(col_name: str) -> bool:
        col = col_name.lower()
        # Exact known list
        if col in requested_time_columns:
            return True
        # Safe pattern matching (word boundaries)
        # avoids matching "lifetime", "update", etc.
        return bool(re.search(r"\b(time|date|timestamp)\b", col)) \
            or col.endswith("_time") \
            or col.endswith("_date")
    for col in out.columns:
        if not is_time_like(col):
            continue
        series = out[col]
        # 1. Already datetime
        if pd.api.types.is_datetime64_any_dtype(series):
            out[col] = series.dt.strftime("%Y-%m-%d %H:%M:%S")
            continue
        # 2. Object → attempt parse
        if pd.api.types.is_object_dtype(series):
            parsed = pd.to_datetime(series, errors="coerce")
            if parsed.notna().mean() > 0.7:
                out[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")
                continue
        # 3. Numeric → epoch handling (microseconds)
        if pd.api.types.is_numeric_dtype(series):
            parsed = pd.to_datetime(series, errors="coerce", unit="us")
            if parsed.notna().mean() > 0.7:
                out[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")
    return out

def parse_s3_uri(s3_uri: str):
    s3_uri = s3_uri.strip()
    if s3_uri.startswith("s3://"):
        s3_uri = s3_uri[5:]
    parts = s3_uri.split("/", 1)
    bucket_name = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    if not bucket_name:
        raise ValueError("S3 destination must include a bucket name")
    return bucket_name, key

def deliver_to_gdrive(report, filename, folder_id, credentials):
    print("Google Drive delivery")
    if not folder_id:
        raise ValueError("Google Drive: folder_id required")
    drive_service = build("drive", "v3", credentials=credentials)
    csv_bytes = report.to_csv(index=False).encode("utf-8")
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv")
    file_metadata = {"name": filename, "parents": [folder_id]}
    created = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
        supportsAllDrives=True
    ).execute()

    gdrive_msg = f"Google Drive file: https://drive.google.com/file/d/{created['id']}/view"
    print(gdrive_msg)
    return gdrive_msg

def deliver_to_s3(report, default_filename, role_name, destination_path, csv_buffer):
    print("Client S3 delivery")
    if not role_name:
        raise ValueError("S3: final_destination_var1 must be the IAM role name")
    if not destination_path:
        raise ValueError(
            "S3: final_destination_var2 must be the full destination path, "
            "for example s3://my-bucket/path/file.csv"
        )
    bucket_name, key = parse_s3_uri(destination_path)
    if not key or key.endswith("/"):
        key = f"{key}{default_filename}" if key else default_filename
    client_s3 = sts_assume_role(role_name)
    response = client_s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    print("put_object response:", response)
    msg = f"Client S3 path: s3://{bucket_name}/{key}"
    print(msg)
    return msg

def deliver_to_snowflake(report, fs_ticket_id, mode_flag, sample):
    print("Snowflake delivery")
    snowflake_table_name = f"LLD_REQ_{fs_ticket_id}"
    # Normalize timestamps safely
    report_clean = normalize_time_columns(report)
    # Determine overwrite vs append
    overwrite_flag = False
    if sample:
        print("Sample run detected → forcing overwrite")
        overwrite_flag = True
    elif mode_flag and mode_flag.lower() == "overwrite":
        print("Overwrite mode requested via var1")
        overwrite_flag = True
    else:
        print("Default append mode")
    print(f"Snowflake write mode: {'OVERWRITE' if overwrite_flag else 'APPEND'}")
    session = snowflake_session()
    session.use_database("PA_OUT")
    session.use_schema("PUBLIC")
    session.write_pandas(
        report_clean,
        snowflake_table_name,
        auto_create_table=True,
        overwrite=overwrite_flag,
        chunk_size=100000,
        parallel=6,
    )
    msg = f"Snowflake table Table: PA_OUT.PUBLIC.{snowflake_table_name}"
    print(msg)
    return msg

def deliver_to_bigquery(report, full_table_id, credentials):
    print("BigQuery delivery")
    import time
    from google.cloud import bigquery
    from google.api_core.exceptions import TooManyRequests, NotFound
    if not full_table_id:
        raise ValueError("BigQuery: table id required")
    bq = bigquery.Client(credentials=credentials, project=credentials.project_id)
    def to_str(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for c in out.columns:
            out[c] = out[c].astype("string").where(out[c].notna(), "")
        return out.reset_index(drop=True)
    def ensure_table_exists_as_string(cols):
        try:
            bq.get_table(full_table_id)
        except NotFound:
            schema = [bigquery.SchemaField(c, "STRING") for c in cols]
            table = bigquery.Table(full_table_id, schema=schema)
            bq.create_table(table)
            print(f"[create] {full_table_id} created with STRING schema.")
    def bq_load(csv_bytes, write_disposition):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=write_disposition,
            allow_quoted_newlines=True,
            max_bad_records=0,
        )
        attempts = 0
        while True:
            try:
                job = bq.load_table_from_file(
                    csv_bytes,
                    full_table_id,
                    job_config=job_config
                )
                job.result()
                if job.errors:
                    raise RuntimeError(job.errors)
                break
            except TooManyRequests:
                attempts += 1
                if attempts >= 6:
                    raise
                time.sleep(2 ** attempts)
    chunk_size = 100000
    loaded_rows = 0
    for i, start_idx in enumerate(range(0, len(report), chunk_size)):
        batch = report.iloc[start_idx:start_idx + chunk_size].copy()
        batch = to_str(batch)
        if i == 0:
            ensure_table_exists_as_string(list(batch.columns))
        buf = io.BytesIO()
        batch.to_csv(buf, index=False, encoding="utf-8")
        buf.seek(0)
        bq_load(
            buf,
            bigquery.WriteDisposition.WRITE_TRUNCATE if i == 0
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        loaded_rows += len(batch)
        print(f"[load] batch {i} rows={len(batch)} cumulative={loaded_rows}")
    msg = f"BigQuery table: {full_table_id}"
    print(msg)
    return msg

def deliver(report, filename, final_destination, var1, var2, csv_buffer, credentials, fs_ticket_id):
    handlers = {
        "snowflake": lambda: deliver_to_snowflake(report, fs_ticket_id, var1, sample),
        "s3": lambda: deliver_to_s3(report, filename, var1, var2, csv_buffer),
        "bigquery": lambda: deliver_to_bigquery(report, var1, credentials),
        "google_drive": lambda: deliver_to_gdrive(report, filename, var1, credentials),
    }
    handler = handlers.get(final_destination)
    if not handler:
        print(f"Unknown destination '{final_destination}'. Defaulting to internal S3 only.")
        return ""
    delivery_msg = handler()
    print("Client delivery complete!!")
    return delivery_msg

def sts_assume_role(role):
    print(f'Assuming role: {role}')
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=f"arn:aws:iam::CENSORED:role/{role}",
        RoleSessionName="AssumeRoleSession1",
    )
    creds = assumed_role_object["Credentials"]
    return boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )


# Actual delivery 
valid_destinations = {"snowflake", "s3", "bigquery", "google_drive"}

if final_destination not in valid_destinations:
    raise ValueError(f"Invalid destination: {final_destination}")

print(f"final_destination={final_destination}")
print(f"final_destination_var1={final_destination_var1}")
print(f"final_destination_var2={final_destination_var2}")

delivery_msg = deliver(
    report=report,
    filename=filename,
    final_destination=final_destination,
    var1=final_destination_var1,
    var2=final_destination_var2,
    csv_buffer=csv_buffer,
    credentials=credentials,
    fs_ticket_id=fs_ticket_id
)

print("LLD complete")

slack_client = slack_connect()
slack_info = slack_getinfo(slack_client)

vidy_row = slack_info[slack_info["first_name"] == "Vidharth"].copy()
vidy_row["first_name"] = "Vidy"
slack_info = pd.concat([slack_info, vidy_row], ignore_index=True)

slack_info["first_name"] = slack_info["first_name"].str.lower()

matches = slack_info.loc[slack_info["first_name"] == owner.lower(), "id"]
target_slack_id = matches.iloc[0] if not matches.empty else ""

SLACK_CHANNEL = "C0B1R4GNPPS" #da-lld-notification

if sample:
    print('Slack message outgoing')
    mention_text = f" CC: <@{target_slack_id}>." if target_slack_id else ""
    delivery_text = f" {delivery_msg}" if delivery_msg else ""
    slack_output = (
        f"Your Sample LLD for (REQ#{fs_ticket_id}) has been generated. "
        f"You can find it here: s3://{bucket}/{internal_s3}.{delivery_text}{mention_text}"
    )
    response = slack_client.chat_postMessage(
        channel=SLACK_CHANNEL,
        text=slack_output
    )
