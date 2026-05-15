with weather as (
  select * from {{ ref('stg_weather') }}
),

final as (
  select
    weather_datetime,
    weather_date,
    weather_hour,
    temperature_celsius,
    temperature_fahrenheit,
    precipitation_mm,
    snowfall_cm,
    windspeed_kmh,
    visibility_metres,
    weather_severity,
    has_snowfall,
    has_precipitation,
    is_below_freezing,

    case
      when snowfall_cm > 2.0          then 5
      when snowfall_cm > 0.5          then 4
      when precipitation_mm > 5.0     then 3
      when precipitation_mm > 1.0     then 2
      when temperature_celsius < -5   then 2
      else 1
    end                                 as weather_impact_score,

    case
      when snowfall_cm > 0 and precipitation_mm > 0   then 'snow_and_rain'
      when snowfall_cm > 0                            then 'snow_only'
      when precipitation_mm > 0                       then 'rain_only'
      else 'dry'
    end                                 as precipitation_type

  from weather
)

select * from final