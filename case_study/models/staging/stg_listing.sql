with source as (
    select * from {{ source('raw', 'listing') }}
),

deduplicated as (
    select
        id as listing_id,
        outlet_id,
        platform_id,
        cast(timestamp as timestamp) as created_at,
        row_number() over (partition by id order by cast(timestamp as timestamp) desc, outlet_id) as rn
    from source
    where id is not null
),

cleaned as (
    select
        listing_id,
        outlet_id,
        platform_id,
        created_at,
        current_timestamp() as dbt_loaded_at
    from deduplicated
    where rn = 1
)

select * from cleaned