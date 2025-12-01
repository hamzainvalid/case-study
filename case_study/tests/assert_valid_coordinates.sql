with outlets_with_listings as (
    select distinct
        outlet_id,
        outlet_latitude,
        outlet_longitude
    from {{ ref('fct_daily_performance') }}
)

select *
from outlets_with_listings
where outlet_latitude is null
   or outlet_longitude is null
   or outlet_latitude not between -90 and 90
   or outlet_longitude not between -180 and 180