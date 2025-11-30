with ranks as (
    select
        cast(rank_date as date) as rank_date,  -- Cast to date
        listing_id,
        rank,
        is_online
    from {{ ref('stg_rank') }}
),

daily_aggregated as (
    select
        rank_date,
        listing_id,
        avg(rank) as avg_daily_rank,
        min(rank) as min_daily_rank,
        max(rank) as max_daily_rank,
        count(*) as rank_snapshot_count,
        sum(case when is_online then 1 else 0 end) as online_snapshot_count,
        sum(case when not is_online then 1 else 0 end) as offline_snapshot_count,
        max(case when is_online then 1 else 0 end) = 1 as was_online_any_time
    from ranks
    group by 1, 2
)

select * from daily_aggregated