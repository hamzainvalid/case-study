with weather as (
    select * from {{ ref('stg_weather') }}
),

daily_aggregated as (
    select
        weather_date,
        outlet_id,
        avg(wind_speed_10m) as avg_wind_speed,
        max(wind_speed_10m) as max_wind_speed,
        avg(temperature_2m) as avg_temperature,
        min(temperature_2m) as min_temperature,
        max(temperature_2m) as max_temperature,
        avg(relative_humidity_2m) as avg_humidity,
        max(relative_humidity_2m) as max_humidity,
        count(*) as weather_reading_count
    from weather
    group by 1, 2
)

select * from daily_aggregated