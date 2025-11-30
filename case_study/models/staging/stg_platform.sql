with source as (
    select * from {{ source('raw', 'platform') }}
)

select
    id as platform_id,
    trim(`group`) as platform_group,
    trim(name) as platform_name,
    upper(trim(country)) as country_code,
    current_timestamp() as dbt_loaded_at
from source
where id is not null