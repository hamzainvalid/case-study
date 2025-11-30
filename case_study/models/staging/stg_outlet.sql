with source as (
    select * from {{ source('raw', 'outlet') }}
),

deduplicated as (
    select
        id as outlet_id,
        org_id,
        trim(name) as outlet_name,
        latitude,
        longitude,
        cast(timestamp as timestamp) as created_at,
        row_number() over (partition by id order by cast(timestamp as timestamp) desc, name) as rn
    from source
    where id is not null
      and latitude between -90 and 90
      and longitude between -180 and 180
),

cleaned as (
    select
        outlet_id,
        org_id,
        outlet_name,
        latitude,
        longitude,
        created_at,
        current_timestamp() as dbt_loaded_at
    from deduplicated
    where rn = 1
)

select * from cleaned