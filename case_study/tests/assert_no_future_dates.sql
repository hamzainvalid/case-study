with all_dates as (
    select order_date as date_value, 'orders' as source
    from {{ ref('int_daily_orders') }}
    union all
    select rating_date as date_value, 'ratings' as source
    from {{ ref('int_daily_ratings') }}
    union all
    select rank_date as date_value, 'ranks' as source
    from {{ ref('int_daily_rank') }}
    union all
    select weather_date as date_value, 'weather' as source
    from {{ ref('int_daily_weather') }}
)

select *
from all_dates
where date_value > current_date