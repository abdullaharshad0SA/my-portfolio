{{ config(materialized='table') }}

-- One row per campaign per activity date.
-- The goal is to make the daily reporting layer useful before it reaches a dashboard: delivery metrics,
-- conversion timing, pacing context, and a few plain health checks.

-- Keep the starting CTEs boring. That makes it easier to see what each upstream model contributes.

with campaign_daily_delivery as (
    select *

    from {{ ref('fct_campaign_daily_delivery') }}
),

campaigns as (
    select *

    from {{ ref('dim_campaigns') }}

    where coalesce(is_deleted, false) = false
),

conversion_paths as (
    select *

    from {{ ref('int_marketing_event_conversion_path') }}
),

-- Standardize dates, IDs, and numeric fields before aggregating.

delivery_cleaned_cte as (
    select
        safe_cast(activity_date as date) as activity_date,
        safe_cast(campaign_id as string) as campaign_id,
        safe_cast(line_item_id as string) as line_item_id,
        coalesce(trim(supply_inventory_type), 'Unknown') as supply_inventory_type,
        coalesce(trim(device_type), 'Unknown') as device_type,
        coalesce(safe_cast(impressions as int64), 0) as impressions,
        coalesce(safe_cast(clicks as int64), 0) as clicks,
        coalesce(safe_cast(conversions as int64), 0) as conversions,
        coalesce(safe_cast(reach_count as int64), 0) as reach_count,
        coalesce(safe_cast(media_cost as float64), 0) as media_cost

    from campaign_daily_delivery

    where activity_date is not null
),

campaigns_cleaned_cte as (
    select
        safe_cast(campaign_id as string) as campaign_id,
        coalesce(trim(campaign_name), 'Unnamed Campaign') as campaign_name,
        coalesce(trim(account_id), 'No Account') as account_id,
        coalesce(trim(channel), 'Unknown') as channel,
        coalesce(trim(campaign_type), 'Unknown') as campaign_type,
        coalesce(trim(status), 'Unknown') as campaign_status,
        coalesce(trim(primary_goal), 'No Goal') as primary_goal,
        safe_cast(primary_goal_amount as float64) as primary_goal_amount,
        coalesce(safe_cast(budget_amount as float64), 0) as budget_amount,
        safe_cast(start_date as date) as campaign_start_date,
        safe_cast(end_date as date) as campaign_end_date

    from campaigns
),

-- Aggregate first, then join. It avoids multiplying delivery rows when conversion history has many events.

delivery_aggregated_cte as (
    select
        delivery_cleaned_cte.activity_date,
        delivery_cleaned_cte.campaign_id,
        sum(delivery_cleaned_cte.impressions) as impressions,
        sum(delivery_cleaned_cte.clicks) as clicks,
        sum(delivery_cleaned_cte.conversions) as platform_conversions,
        sum(delivery_cleaned_cte.reach_count) as reach_count,
        sum(delivery_cleaned_cte.media_cost) as media_cost,
        count(distinct delivery_cleaned_cte.line_item_id) as line_item_count,
        string_agg(distinct delivery_cleaned_cte.supply_inventory_type, ' / ' order by delivery_cleaned_cte.supply_inventory_type) as supply_inventory_type_list,
        string_agg(distinct delivery_cleaned_cte.device_type, ' / ' order by delivery_cleaned_cte.device_type) as device_type_list

    from delivery_cleaned_cte

    group by
        delivery_cleaned_cte.activity_date,
        delivery_cleaned_cte.campaign_id
),

conversion_path_summary_cte as (
    select
        conversion_paths.conversion_date as activity_date,
        conversion_paths.campaign_id,
        count(distinct conversion_paths.conversion_event_id) as deduped_conversion_count,
        sum(conversion_paths.conversion_value) as deduped_conversion_value,
        avg(conversion_paths.hours_from_first_impression_to_conversion) as avg_hours_from_first_impression_to_conversion,
        countif(conversion_paths.has_click_before_conversion) as click_path_conversion_count,
        countif(conversion_paths.has_impression_before_conversion and not conversion_paths.has_click_before_conversion) as view_path_conversion_count

    from conversion_paths

    group by
        conversion_paths.conversion_date,
        conversion_paths.campaign_id
),

campaign_day_enriched_cte as (
    select
        delivery_aggregated_cte.activity_date,
        delivery_aggregated_cte.campaign_id,
        coalesce(campaigns_cleaned_cte.campaign_name, 'Unknown Campaign') as campaign_name,
        coalesce(campaigns_cleaned_cte.account_id, 'No Account') as account_id,
        coalesce(campaigns_cleaned_cte.channel, 'Unknown') as channel,
        coalesce(campaigns_cleaned_cte.campaign_type, 'Unknown') as campaign_type,
        coalesce(campaigns_cleaned_cte.campaign_status, 'Unknown') as campaign_status,
        coalesce(campaigns_cleaned_cte.primary_goal, 'No Goal') as primary_goal,
        campaigns_cleaned_cte.primary_goal_amount,
        campaigns_cleaned_cte.budget_amount,
        campaigns_cleaned_cte.campaign_start_date,
        campaigns_cleaned_cte.campaign_end_date,
        delivery_aggregated_cte.impressions,
        delivery_aggregated_cte.clicks,
        delivery_aggregated_cte.platform_conversions,
        coalesce(conversion_path_summary_cte.deduped_conversion_count, 0) as deduped_conversion_count,
        coalesce(conversion_path_summary_cte.deduped_conversion_value, 0) as deduped_conversion_value,
        delivery_aggregated_cte.reach_count,
        delivery_aggregated_cte.media_cost,
        delivery_aggregated_cte.line_item_count,
        delivery_aggregated_cte.supply_inventory_type_list,
        delivery_aggregated_cte.device_type_list,
        conversion_path_summary_cte.avg_hours_from_first_impression_to_conversion,
        coalesce(conversion_path_summary_cte.click_path_conversion_count, 0) as click_path_conversion_count,
        coalesce(conversion_path_summary_cte.view_path_conversion_count, 0) as view_path_conversion_count

    from delivery_aggregated_cte

    left outer join campaigns_cleaned_cte
        on campaigns_cleaned_cte.campaign_id = delivery_aggregated_cte.campaign_id

    left outer join conversion_path_summary_cte
        on
            conversion_path_summary_cte.campaign_id = delivery_aggregated_cte.campaign_id
            and conversion_path_summary_cte.activity_date = delivery_aggregated_cte.activity_date
),

campaign_day_metrics_cte as (
    select
        campaign_day_enriched_cte.*,
        sum(campaign_day_enriched_cte.media_cost) over (
            partition by campaign_day_enriched_cte.campaign_id
            order by campaign_day_enriched_cte.activity_date
            rows between unbounded preceding and current row
        ) as cumulative_media_cost,
        safe_divide(campaign_day_enriched_cte.clicks, campaign_day_enriched_cte.impressions) as click_through_rate,
        safe_divide(campaign_day_enriched_cte.deduped_conversion_count, campaign_day_enriched_cte.clicks) as click_to_conversion_rate,
        safe_divide(campaign_day_enriched_cte.media_cost, campaign_day_enriched_cte.deduped_conversion_count) as cost_per_deduped_conversion,
        safe_divide(campaign_day_enriched_cte.impressions, campaign_day_enriched_cte.reach_count) as average_frequency,

        greatest(date_diff(campaign_day_enriched_cte.campaign_end_date, campaign_day_enriched_cte.campaign_start_date, day) + 1, 1) as flight_day_count,
        greatest(date_diff(least(campaign_day_enriched_cte.activity_date, campaign_day_enriched_cte.campaign_end_date), campaign_day_enriched_cte.campaign_start_date, day) + 1, 0) as elapsed_flight_day_count

    from campaign_day_enriched_cte
),

health_flags_cte as (
    select
        campaign_day_metrics_cte.*,
        safe_divide(campaign_day_metrics_cte.elapsed_flight_day_count, campaign_day_metrics_cte.flight_day_count) as expected_budget_progress,
        safe_divide(campaign_day_metrics_cte.cumulative_media_cost, nullif(campaign_day_metrics_cte.budget_amount, 0)) as actual_budget_progress,
        campaign_day_metrics_cte.primary_goal = 'No Goal' as has_missing_primary_goal,
        campaign_day_metrics_cte.impressions > 0 and campaign_day_metrics_cte.media_cost = 0 as has_delivery_without_spend,
        campaign_day_metrics_cte.impressions = 0 and campaign_day_metrics_cte.media_cost > 0 as has_spend_without_delivery,
        campaign_day_metrics_cte.click_through_rate > 0.30 as has_unusually_high_click_through_rate,
        campaign_day_metrics_cte.platform_conversions != campaign_day_metrics_cte.deduped_conversion_count as has_conversion_count_gap,

        -- This is not trying to make a judgment for the operator. It just points to rows worth checking first.
        abs(
            safe_divide(campaign_day_metrics_cte.cumulative_media_cost, nullif(campaign_day_metrics_cte.budget_amount, 0))
            - safe_divide(campaign_day_metrics_cte.elapsed_flight_day_count, campaign_day_metrics_cte.flight_day_count)
        ) > 0.20 as has_budget_pacing_gap

    from campaign_day_metrics_cte
),

final_cte as (
    select
        health_flags_cte.activity_date,
        health_flags_cte.campaign_id,
        health_flags_cte.campaign_name,
        health_flags_cte.account_id,
        health_flags_cte.channel,
        health_flags_cte.campaign_type,
        health_flags_cte.campaign_status,
        health_flags_cte.primary_goal,
        health_flags_cte.primary_goal_amount,
        health_flags_cte.budget_amount,
        health_flags_cte.campaign_start_date,
        health_flags_cte.campaign_end_date,
        health_flags_cte.impressions,
        health_flags_cte.clicks,
        health_flags_cte.platform_conversions,
        health_flags_cte.deduped_conversion_count,
        health_flags_cte.deduped_conversion_value,
        health_flags_cte.reach_count,
        health_flags_cte.average_frequency,
        health_flags_cte.media_cost,
        health_flags_cte.cumulative_media_cost,
        health_flags_cte.click_through_rate,
        health_flags_cte.click_to_conversion_rate,
        health_flags_cte.cost_per_deduped_conversion,
        health_flags_cte.expected_budget_progress,
        health_flags_cte.actual_budget_progress,
        health_flags_cte.avg_hours_from_first_impression_to_conversion,
        health_flags_cte.click_path_conversion_count,
        health_flags_cte.view_path_conversion_count,
        health_flags_cte.line_item_count,
        health_flags_cte.supply_inventory_type_list,
        health_flags_cte.device_type_list,
        health_flags_cte.has_missing_primary_goal,
        health_flags_cte.has_delivery_without_spend,
        health_flags_cte.has_spend_without_delivery,
        health_flags_cte.has_unusually_high_click_through_rate,
        health_flags_cte.has_conversion_count_gap,
        health_flags_cte.has_budget_pacing_gap,

        100
            - if(health_flags_cte.has_missing_primary_goal, 20, 0)
            - if(health_flags_cte.has_delivery_without_spend, 20, 0)
            - if(health_flags_cte.has_spend_without_delivery, 20, 0)
            - if(health_flags_cte.has_unusually_high_click_through_rate, 10, 0)
            - if(health_flags_cte.has_conversion_count_gap, 15, 0)
            - if(health_flags_cte.has_budget_pacing_gap, 15, 0) as reporting_health_score,

        case
            when health_flags_cte.has_missing_primary_goal then 'Missing goal'
            when health_flags_cte.has_delivery_without_spend then 'Delivery without spend'
            when health_flags_cte.has_spend_without_delivery then 'Spend without delivery'
            when health_flags_cte.has_conversion_count_gap then 'Conversion count gap'
            when health_flags_cte.has_budget_pacing_gap then 'Budget pacing gap'
            when health_flags_cte.has_unusually_high_click_through_rate then 'Rate outlier'
            else 'Looks clean'
        end as primary_health_note,

        case
            when health_flags_cte.has_missing_primary_goal then 1
            when health_flags_cte.has_delivery_without_spend then 2
            when health_flags_cte.has_spend_without_delivery then 3
            when health_flags_cte.has_conversion_count_gap then 4
            when health_flags_cte.has_budget_pacing_gap then 5
            when health_flags_cte.has_unusually_high_click_through_rate then 6
            else 999
        end as primary_health_note_sort,

        current_timestamp() as updated_timestamp

    from health_flags_cte
)

select *
from final_cte
