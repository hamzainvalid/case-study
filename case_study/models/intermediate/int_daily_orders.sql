with order_daily as (
    select
        date(order_date) as order_date,  -- Use DATE() function instead of CAST
        listing_id,
        daily_orders
    from {{ ref('stg_orders_daily') }}
    where order_date is not null
),

orders_actual as (
    select
        date(order_date) as order_date,  -- Use DATE() function instead of CAST
        listing_id,
        count(distinct order_id) as actual_orders,
        count(distinct case when status = 'completed' then order_id end) as completed_orders,
        count(distinct case when status = 'cancelled' then order_id end) as cancelled_orders
    from {{ ref('stg_orders') }}
    where order_date is not null
    group by 1, 2
),

combined as (
    select
        coalesce(od.order_date, oa.order_date) as order_date,
        coalesce(od.listing_id, oa.listing_id) as listing_id,
        coalesce(od.daily_orders, 0) as aggregated_orders,
        coalesce(oa.actual_orders, 0) as actual_orders,
        coalesce(oa.completed_orders, 0) as completed_orders,
        coalesce(oa.cancelled_orders, 0) as cancelled_orders
    from order_daily od
    full outer join orders_actual oa
        on od.order_date = oa.order_date
        and od.listing_id = oa.listing_id
)

select * from combined