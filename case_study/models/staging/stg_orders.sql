with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned_orders as (
    select
        order_id,
        listing_id,
        -- Use PARSE_TIMESTAMP for more control
        case
            when safe_cast(placed_at as timestamp) is not null
            then safe_cast(placed_at as timestamp)
            else timestamp('1970-01-01')  -- Default value for invalid dates
        end as placed_at,
        -- Extract date from valid timestamp
        case
            when safe_cast(placed_at as timestamp) is not null
            then safe_cast(placed_at as date)
            else date('1970-01-01')  -- Default value for invalid dates
        end as order_date,
        lower(trim(status)) as status,
        current_timestamp() as dbt_loaded_at
    from source
    where order_id is not null
      and listing_id is not null
)

select * from cleaned_orders