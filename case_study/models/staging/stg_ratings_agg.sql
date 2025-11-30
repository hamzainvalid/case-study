with source as (
    select * from {{ source('raw', 'ratings_agg') }}
),

deduplicated as (
    select
        cast(date as date) as rating_date,
        listing_id,
        cnt_ratings as rating_count,
        avg_rating as average_rating,
        row_number() over (
            partition by cast(date as date), listing_id
            order by cnt_ratings desc, avg_rating desc
        ) as rn
    from source
    where date is not null
      and listing_id is not null
      and cnt_ratings >= 0
      and avg_rating between 0 and 5
),

cleaned as (
    select
        rating_date,
        listing_id,
        rating_count,
        average_rating,
        current_timestamp() as dbt_loaded_at
    from deduplicated
    where rn = 1
)

select * from cleaned