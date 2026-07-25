{{ config(materialized='table') }}

-- One row per deduped conversion event.
-- This model keeps the path logic small enough to review: the conversion, the touch history before it,
-- and the basic timing fields someone would need for campaign or product measurement.

-- Pull only the base inputs first. The more opinionated logic starts after the fields are cleaned.

with marketing_events as (
    select *

    from {{ ref('stg_marketing_events') }}

    where
        event_at is not null
        and coalesce(is_test_event, false) = false
),

campaigns as (
    select *

    from {{ ref('dim_campaigns') }}

    where coalesce(is_deleted, false) = false
),

-- Clean the source fields once so the rest of the model does not repeat casting and fallback logic.

events_cleaned_cte as (
    select
        safe_cast(event_id as string) as event_id,
        safe_cast(anonymous_user_id as string) as anonymous_user_id,
        safe_cast(auction_id as string) as auction_id,
        safe_cast(campaign_id as string) as campaign_id,
        safe_cast(line_item_id as string) as line_item_id,
        safe_cast(creative_id as string) as creative_id,
        safe_cast(conversion_tracker_id as string) as conversion_tracker_id,
        safe_cast(conversion_event_id as string) as conversion_event_id,
        safe_cast(order_id as string) as order_id,
        timestamp(event_at) as event_at,
        date(event_at) as event_date,
        lower(trim(event_type)) as event_type,
        coalesce(trim(supply_inventory_type), 'Unknown') as supply_inventory_type,
        coalesce(trim(device_type), 'Unknown') as device_type,
        coalesce(trim(country), 'Unknown') as country,
        coalesce(trim(region), 'Unknown') as region,
        coalesce(trim(city), 'Unknown') as city,
        safe_cast(cost_micros as float64) as cost_micros,
        safe_cast(conversion_value as float64) as conversion_value,
        coalesce(lower(trim(conversion_count_type)), 'multiple') as conversion_count_type,
        coalesce(has_impression, lower(trim(event_type)) = 'impression') as has_impression,
        coalesce(has_click, lower(trim(event_type)) = 'click') as has_click,
        coalesce(has_conversion, lower(trim(event_type)) = 'conversion') as has_conversion

    from marketing_events
),

campaigns_cleaned_cte as (
    select
        safe_cast(campaign_id as string) as campaign_id,
        coalesce(trim(campaign_name), 'Unnamed Campaign') as campaign_name,
        coalesce(trim(account_id), 'No Account') as account_id,
        coalesce(trim(channel), 'Unknown') as channel,
        coalesce(trim(campaign_type), 'Unknown') as campaign_type,
        coalesce(trim(primary_goal), 'No Goal') as primary_goal,
        coalesce(trim(status), 'Unknown') as campaign_status,
        safe_cast(start_date as date) as campaign_start_date,
        safe_cast(end_date as date) as campaign_end_date

    from campaigns
),

conversion_events_cte as (
    select
        events_cleaned_cte.event_id as source_event_id,
        coalesce(events_cleaned_cte.conversion_event_id, events_cleaned_cte.event_id) as conversion_event_id,
        events_cleaned_cte.anonymous_user_id,
        events_cleaned_cte.auction_id,
        events_cleaned_cte.campaign_id,
        events_cleaned_cte.conversion_tracker_id,
        events_cleaned_cte.order_id,
        events_cleaned_cte.event_at as conversion_at,
        events_cleaned_cte.event_date as conversion_date,
        coalesce(events_cleaned_cte.conversion_value, 0) as conversion_value,
        events_cleaned_cte.conversion_count_type,

        -- Single-count trackers should only keep the first conversion for the same user and tracker.
        row_number() over (
            partition by
                events_cleaned_cte.anonymous_user_id,
                events_cleaned_cte.conversion_tracker_id,
                coalesce(events_cleaned_cte.order_id, events_cleaned_cte.conversion_event_id, events_cleaned_cte.event_id)
            order by events_cleaned_cte.event_at
        ) as conversion_rank

    from events_cleaned_cte

    where events_cleaned_cte.has_conversion
),

deduped_conversions_cte as (
    select *

    from conversion_events_cte

    where
        conversion_count_type != 'once'
        or conversion_rank = 1
),

pre_conversion_events_cte as (
    select
        deduped_conversions_cte.conversion_event_id,
        deduped_conversions_cte.anonymous_user_id,
        deduped_conversions_cte.conversion_tracker_id,
        deduped_conversions_cte.campaign_id as conversion_campaign_id,
        deduped_conversions_cte.conversion_at,
        deduped_conversions_cte.conversion_date,
        deduped_conversions_cte.conversion_value,
        events_cleaned_cte.event_id,
        events_cleaned_cte.auction_id,
        events_cleaned_cte.campaign_id as touch_campaign_id,
        events_cleaned_cte.line_item_id,
        events_cleaned_cte.creative_id,
        events_cleaned_cte.event_at as touch_at,
        events_cleaned_cte.event_date as touch_date,
        events_cleaned_cte.supply_inventory_type,
        events_cleaned_cte.device_type,
        events_cleaned_cte.country,
        events_cleaned_cte.region,
        events_cleaned_cte.city,
        events_cleaned_cte.has_impression,
        events_cleaned_cte.has_click,
        safe_divide(coalesce(events_cleaned_cte.cost_micros, 0), 1000000) as media_cost

    from deduped_conversions_cte

    left outer join events_cleaned_cte
        on
            events_cleaned_cte.anonymous_user_id = deduped_conversions_cte.anonymous_user_id
            and events_cleaned_cte.event_at <= deduped_conversions_cte.conversion_at
            and events_cleaned_cte.event_at >= timestamp_sub(deduped_conversions_cte.conversion_at, interval 60 day)
            and (events_cleaned_cte.has_impression or events_cleaned_cte.has_click)
),

-- Roll the touch history up to the conversion grain before adding dashboard-facing labels.

conversion_path_rollup_cte as (
    select
        pre_conversion_events_cte.conversion_event_id,
        pre_conversion_events_cte.anonymous_user_id,
        pre_conversion_events_cte.conversion_tracker_id,
        pre_conversion_events_cte.conversion_campaign_id,
        pre_conversion_events_cte.conversion_at,
        pre_conversion_events_cte.conversion_date,
        max(pre_conversion_events_cte.conversion_value) as conversion_value,
        min(if(pre_conversion_events_cte.has_impression, pre_conversion_events_cte.touch_at, null)) as first_impression_at,
        max(if(pre_conversion_events_cte.has_impression, pre_conversion_events_cte.touch_at, null)) as last_impression_at,
        min(if(pre_conversion_events_cte.has_click, pre_conversion_events_cte.touch_at, null)) as first_click_at,
        max(if(pre_conversion_events_cte.has_click, pre_conversion_events_cte.touch_at, null)) as last_click_at,
        count(distinct if(pre_conversion_events_cte.has_impression, pre_conversion_events_cte.event_id, null)) as impression_touch_count,
        count(distinct if(pre_conversion_events_cte.has_click, pre_conversion_events_cte.event_id, null)) as click_touch_count,
        count(distinct pre_conversion_events_cte.touch_campaign_id) as touched_campaign_count,
        count(distinct pre_conversion_events_cte.creative_id) as touched_creative_count,
        sum(coalesce(pre_conversion_events_cte.media_cost, 0)) as pre_conversion_media_cost,
        string_agg(distinct coalesce(pre_conversion_events_cte.supply_inventory_type, 'Unknown'), ' / ' order by coalesce(pre_conversion_events_cte.supply_inventory_type, 'Unknown')) as supply_inventory_type_list,
        string_agg(distinct coalesce(pre_conversion_events_cte.device_type, 'Unknown'), ' / ' order by coalesce(pre_conversion_events_cte.device_type, 'Unknown')) as device_type_list

    from pre_conversion_events_cte

    group by
        pre_conversion_events_cte.conversion_event_id,
        pre_conversion_events_cte.anonymous_user_id,
        pre_conversion_events_cte.conversion_tracker_id,
        pre_conversion_events_cte.conversion_campaign_id,
        pre_conversion_events_cte.conversion_at,
        pre_conversion_events_cte.conversion_date
),

path_classification_cte as (
    select
        conversion_path_rollup_cte.*,
        timestamp_diff(conversion_path_rollup_cte.conversion_at, conversion_path_rollup_cte.first_impression_at, hour) as hours_from_first_impression_to_conversion,
        timestamp_diff(conversion_path_rollup_cte.conversion_at, conversion_path_rollup_cte.first_click_at, hour) as hours_from_first_click_to_conversion,
        conversion_path_rollup_cte.impression_touch_count > 0 as has_impression_before_conversion,
        conversion_path_rollup_cte.click_touch_count > 0 as has_click_before_conversion,

        case
            when conversion_path_rollup_cte.click_touch_count > 0 then 'Click path'
            when conversion_path_rollup_cte.impression_touch_count > 0 then 'View path'
            else 'Conversion only'
        end as conversion_path_type,

        case
            when conversion_path_rollup_cte.first_impression_at is null then 'No prior impression'
            when timestamp_diff(conversion_path_rollup_cte.conversion_at, conversion_path_rollup_cte.first_impression_at, hour) < 24 then '< 1 day'
            when timestamp_diff(conversion_path_rollup_cte.conversion_at, conversion_path_rollup_cte.first_impression_at, day) <= 5 then '1-5 days'
            when timestamp_diff(conversion_path_rollup_cte.conversion_at, conversion_path_rollup_cte.first_impression_at, day) <= 10 then '6-10 days'
            when timestamp_diff(conversion_path_rollup_cte.conversion_at, conversion_path_rollup_cte.first_impression_at, day) <= 20 then '11-20 days'
            when timestamp_diff(conversion_path_rollup_cte.conversion_at, conversion_path_rollup_cte.first_impression_at, day) <= 30 then '21-30 days'
            else '> 30 days'
        end as impression_to_conversion_bucket

    from conversion_path_rollup_cte
),

final_cte as (
    select
        path_classification_cte.conversion_event_id,
        path_classification_cte.anonymous_user_id,
        path_classification_cte.conversion_tracker_id,
        path_classification_cte.conversion_campaign_id as campaign_id,
        coalesce(campaigns_cleaned_cte.campaign_name, 'Unknown Campaign') as campaign_name,
        coalesce(campaigns_cleaned_cte.account_id, 'No Account') as account_id,
        coalesce(campaigns_cleaned_cte.channel, 'Unknown') as channel,
        coalesce(campaigns_cleaned_cte.campaign_type, 'Unknown') as campaign_type,
        coalesce(campaigns_cleaned_cte.primary_goal, 'No Goal') as primary_goal,
        coalesce(campaigns_cleaned_cte.campaign_status, 'Unknown') as campaign_status,
        path_classification_cte.conversion_at,
        path_classification_cte.conversion_date,
        path_classification_cte.conversion_value,
        path_classification_cte.first_impression_at,
        path_classification_cte.last_impression_at,
        path_classification_cte.first_click_at,
        path_classification_cte.last_click_at,
        path_classification_cte.hours_from_first_impression_to_conversion,
        path_classification_cte.hours_from_first_click_to_conversion,
        path_classification_cte.impression_to_conversion_bucket,
        path_classification_cte.conversion_path_type,
        path_classification_cte.has_impression_before_conversion,
        path_classification_cte.has_click_before_conversion,
        path_classification_cte.impression_touch_count,
        path_classification_cte.click_touch_count,
        path_classification_cte.touched_campaign_count,
        path_classification_cte.touched_creative_count,
        path_classification_cte.pre_conversion_media_cost,
        path_classification_cte.supply_inventory_type_list,
        path_classification_cte.device_type_list,
        current_timestamp() as updated_timestamp

    from path_classification_cte

    left outer join campaigns_cleaned_cte
        on campaigns_cleaned_cte.campaign_id = path_classification_cte.conversion_campaign_id
)

select *
from final_cte
