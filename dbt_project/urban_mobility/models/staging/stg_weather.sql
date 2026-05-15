with source as (
  select * from {{ source('raw', 'weather_raw') }}
),

cleaned as (
  select
    parse_timestamp('%Y-%m-%dT%H:%M', datetime_hour)                          as weather_datetime,
    date(parse_timestamp('%Y-%m-%dT%H:%M', datetime_hour))                    as weather_date,
    extract(hour from parse_timestamp('%Y-%m-%dT%H:%M', datetime_hour))       as weather_hour,
    
    -- temperature
    cast(temperature_2m as float64)                                           as temperature_celsius,
    round(cast(temperature_2m as float64) * 9/5 + 32, 1)                      as temperature_fahrenheit,
    
    -- precipitation
    cast(precipitation as float64)                                            as precipitation_mm,
    cast(snowfall as float64)                                                 as snowfall_cm,
    cast(windspeed_10m as float64)                                            as windspeed_kmh,
    cast(visibility as float64)                                               as visibility_metres,
    cast(weathercode as int64)                                                as weather_code,
    
    -- classifies each hour into severity buckets
    case
        when cast(snowfall as float64) > 2.0        then 'heavy_snow'
        when cast(snowfall as float64) > 0.5        then 'light_snow'
        when cast(precipitation as float64) > 5.0   then 'heavy_rain'
        when cast(precipitation as float64) > 1.0   then 'light_rain'
        when cast(temperature_2m as float64) < -5   then 'extreme_cold'
        when cast(temperature_2m as float64) > 30   then 'extreme_heat'
        when cast(temperature_2m as float64) > 22   then 'warm'
        else 'clear'
    end                                                                       as weather_severity,
    
    -- true/false flags for easy filtering downstream
    cast(snowfall as float64) > 0                                             as has_snowfall,
    cast(precipitation as float64) > 0                                        as has_precipitation,
    cast(temperature_2m as float64) < 0                                       as is_below_freezing,
    
    -- location metadata
    cast(latitude as float64)                                                 as latitude,
    cast(longitude as float64)                                                as longitude,
    timezone
  
  from source
)

select * from cleaned