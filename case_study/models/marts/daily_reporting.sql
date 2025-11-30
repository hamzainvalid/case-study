{{
    config(
        materialized='table',
        unique_key=['report_date', 'listing_id']
    )
}}

with listings as (
    select * from {{ ref('stg_listing') }}
),

outlets as (
    select * from {{ ref('stg_outlet') }}
),

orgs as (
    select * from {{ ref('stg_org') }}
),

platforms as (
    select * from {{ ref('stg_platform') }}
),

orders as (
    select * from {{ ref('int_daily_orders') }}
),

ratings as (
    select * from {{ ref('int_daily_ratings') }}
),

ranks as (
    select * from {{ ref('int_daily_rank') }}
),

weather as (
    select * from {{ ref('int_daily_weather') }}
),

date_spine as (
    select distinct report_date
    from (
        select order_date as report_date from orders
        union all
        select rating_date as report_date from ratings
        union all
        select rank_date as report_date from ranks
    )
),

listing_dates as (
    -- Create cartesian product of listings and dates (only for active period)
    select
        ds.report_date,
        l.listing_id,
        l.outlet_id,
        l.platform_id
    from date_spine ds
    cross join listings l
    where ds.report_date >= cast(l.created_at as date)
),

final as (
    select
        -- Date and identifiers
        ld.report_date,
        ld.listing_id,
        ld.outlet_id,
        ld.platform_id,

        -- Organization information
        o.org_id,
        o.org_name,

        -- Outlet information
        out.outlet_name,
        out.latitude as outlet_latitude,
        out.longitude as outlet_longitude,

        -- Platform information
        p.platform_name,
        p.platform_group,
        p.country_code,

        -- Order metrics
        coalesce(ord.aggregated_orders, 0) as aggregated_orders,
        coalesce(ord.actual_orders, 0) as actual_orders,
        coalesce(ord.completed_orders, 0) as completed_orders,
        coalesce(ord.cancelled_orders, 0) as cancelled_orders,
        case
            when ord.actual_orders > 0
            then round(cast(ord.completed_orders as float64) / ord.actual_orders, 4)
            else null
        end as completion_rate,

        -- Rating metrics (cumulative)
        rat.cumulative_rating_count,
        rat.cumulative_average_rating,

        -- Rating metrics (daily delta)
        coalesce(rat.daily_rating_count_delta, 0) as daily_new_ratings,
        rat.daily_rating_delta,

        -- Rank metrics
        rnk.avg_daily_rank,
        rnk.min_daily_rank as best_rank_of_day,
        rnk.max_daily_rank as worst_rank_of_day,
        rnk.was_online_any_time as was_online,
        case
            when rnk.rank_snapshot_count > 0
            then round(cast(rnk.online_snapshot_count as float64) / rnk.rank_snapshot_count, 4)
            else null
        end as online_percentage,

        -- Weather metrics
        w.avg_temperature,
        w.min_temperature,
        w.max_temperature,
        w.avg_wind_speed,
        w.max_wind_speed,
        w.avg_humidity,
        w.max_humidity,

        -- Calculated business metrics
        case
            when coalesce(rat.cumulative_average_rating, 0) < 3.5 then 'Low'
            when coalesce(rat.cumulative_average_rating, 0) < 4.2 then 'Medium'
            else 'High'
        end as rating_tier,

        case
            when coalesce(ord.actual_orders, 0) = 0 then 'No Orders'
            when coalesce(ord.actual_orders, 0) < 10 then 'Low Volume'
            when coalesce(ord.actual_orders, 0) < 50 then 'Medium Volume'
            else 'High Volume'
        end as order_volume_tier,

        case
            when w.avg_temperature < 10 then 'Cold'
            when w.avg_temperature < 25 then 'Moderate'
            else 'Hot'
        end as temperature_category,

        case
            when w.avg_wind_speed < 10 then 'Calm'
            when w.avg_wind_speed < 30 then 'Moderate Wind'
            else 'Strong Wind'
        end as wind_category,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from listing_dates ld
    left join outlets out on ld.outlet_id = out.outlet_id
    left join orgs o on out.org_id = o.org_id
    left join platforms p on ld.platform_id = p.platform_id
    left join orders ord
        on ld.report_date = ord.order_date
        and ld.listing_id = ord.listing_id
    left join ratings rat
        on ld.report_date = rat.rating_date
        and ld.listing_id = rat.listing_id
    left join ranks rnk
        on ld.report_date = rnk.rank_date
        and ld.listing_id = rnk.listing_id
    left join weather w
        on ld.report_date = w.weather_date
        and ld.outlet_id = w.outlet_id
)

select * from final