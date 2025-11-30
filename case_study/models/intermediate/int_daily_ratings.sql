with ratings as (
    select
        cast(rating_date as date) as rating_date,  -- Cast to date
        listing_id,
        rating_count,
        average_rating
    from {{ ref('stg_ratings_agg') }}
),

with_deltas as (
    select
        rating_date,
        listing_id,
        rating_count as cumulative_rating_count,
        average_rating as cumulative_average_rating,
        rating_count - lag(rating_count, 1, 0) over (
            partition by listing_id
            order by rating_date
        ) as daily_rating_count_delta,
        average_rating - lag(average_rating, 1) over (
            partition by listing_id
            order by rating_date
        ) as daily_rating_delta
    from ratings
)

select * from with_deltas