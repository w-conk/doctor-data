-- Staging model for weather data
-- Reads from raw JSON files and creates a clean table

{{ config(materialized='view') }}

with raw_weather as (
    select
        value:city::string as city,
        value:country::string as country,
        value:timestamp::timestamp as timestamp,
        value:temperature::float as temperature_celsius,
        value:feels_like::float as feels_like_celsius,
        value:humidity::int as humidity_percent,
        value:pressure::int as pressure_hpa,
        value:wind_speed::float as wind_speed_ms,
        value:wind_direction::int as wind_direction_deg,
        value:weather_main::string as weather_condition,
        value:weather_description::string as weather_description,
        value:clouds::int as cloud_coverage_percent,
        value:visibility::int as visibility_meters,
        value:sunrise::timestamp as sunrise,
        value:sunset::timestamp as sunset,
        value:ingested_at::timestamp as ingested_at
    from read_json_auto('{{ var("raw_data_path") }}/weather/*.json')
)

select
    city,
    country,
    timestamp,
    temperature_celsius,
    feels_like_celsius,
    humidity_percent,
    pressure_hpa,
    wind_speed_ms,
    wind_direction_deg,
    weather_condition,
    weather_description,
    cloud_coverage_percent,
    visibility_meters,
    sunrise,
    sunset,
    ingested_at,
    current_timestamp() as transformed_at
from raw_weather
