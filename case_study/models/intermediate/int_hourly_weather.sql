with weather as (
    select * from {{ ref('stg_weather') }}
)

select
    weather_date,
    weather_hour,
    outlet_id,
    avg(wind_speed_10m) as avg_wind_speed,
    avg(temperature_2m) as avg_temperature,
    avg(relative_humidity_2m) as avg_humidity,
    count(*) as reading_count
from weather
group by 1, 2, 3