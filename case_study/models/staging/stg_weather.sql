with source as (
    select * from {{ source('raw', 'weather') }}
)

select
    outlet_id,
    cast(timestamp as timestamp) as weather_timestamp,
    cast(timestamp as date) as weather_date,
    extract(hour from cast(timestamp as timestamp)) as weather_hour,
    wind_speed_10m,
    temperature_2m,
    relative_humidity_2m,
    current_timestamp() as dbt_loaded_at
from source
where outlet_id is not null
  and timestamp is not null