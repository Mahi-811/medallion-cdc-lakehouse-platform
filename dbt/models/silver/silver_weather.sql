{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'api', 'weather']
    )
}}

with raw_weather as (
    select
        _loaded_at,
        _source,
        _batch_id,
        cast(metric_date as date) as metric_date,
        trim(city) as city,
        trim(region_code) as region_code,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(temp_max_c as decimal(5, 2)) as temp_max_c,
        cast(temp_min_c as decimal(5, 2)) as temp_min_c,
        cast(temp_mean_c as decimal(5, 2)) as temp_mean_c,
        cast(coalesce(precipitation_mm, 0.0) as decimal(5, 2)) as precipitation_mm,
        cast(wind_speed_max_kmh as decimal(5, 2)) as wind_speed_max_kmh,
        row_number() over (
            partition by metric_date, city
            order by _loaded_at desc
        ) as rn
    from {{ source('bronze', 'bronze_weather_api') }}
)

select
    metric_date,
    city,
    region_code,
    latitude,
    longitude,
    temp_max_c,
    temp_min_c,
    temp_mean_c,
    precipitation_mm,
    wind_speed_max_kmh,
    case
        when precipitation_mm > 5.0 then 'RAINY'
        when temp_max_c > 30.0 then 'HOT'
        when temp_min_c < 5.0 then 'COLD'
        else 'PLEASANT'
    end as weather_condition
from raw_weather
where rn = 1
