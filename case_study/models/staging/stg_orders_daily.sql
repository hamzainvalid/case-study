with source as (
    select * from {{ source('raw', 'orders_daily') }}
),

deduplicated as (
    select
        cast(date as date) as order_date,
        listing_id,
        orders as daily_orders,
        cast(timestamp as timestamp) as snapshot_at,
        row_number() over (
            partition by cast(date as date), listing_id
            order by cast(timestamp as timestamp) desc, orders desc
        ) as rn
    from source
    where date is not null
      and listing_id is not null
),

cleaned as (
    select
        order_date,
        listing_id,
        daily_orders,
        snapshot_at,
        current_timestamp() as dbt_loaded_at
    from deduplicated
    where rn = 1
)

select * from cleaned