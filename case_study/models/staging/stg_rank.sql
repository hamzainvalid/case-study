with source as (
    select * from {{ source('raw', 'rank') }}
),

deduplicated as (
    select
        listing_id,
        cast(date as date) as rank_date,
        cast(timestamp as timestamp) as snapshot_at,
        is_online,
        rank,
        row_number() over (
            partition by listing_id, cast(date as date), cast(timestamp as timestamp)
            order by rank, is_online desc
        ) as rn
    from source
    where listing_id is not null
      and date is not null
      and rank >= 0
),

cleaned as (
    select
        listing_id,
        rank_date,
        snapshot_at,
        is_online,
        rank,
        current_timestamp() as dbt_loaded_at
    from deduplicated
    where rn = 1
)

select * from cleaned