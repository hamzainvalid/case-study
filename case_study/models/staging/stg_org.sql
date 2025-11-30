with source as (
    select * from {{ source('raw', 'org') }}
),

deduplicated as (
    select
        id as org_id,
        trim(name) as org_name,
        cast(timestamp as timestamp) as created_at,
        row_number() over (partition by id order by cast(timestamp as timestamp) desc, name) as rn
    from source
    where id is not null
),

cleaned as (
    select
        org_id,
        org_name,
        created_at,
        current_timestamp() as dbt_loaded_at
    from deduplicated
    where rn = 1
)

select * from cleaned