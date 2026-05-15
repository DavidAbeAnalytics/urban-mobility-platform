-- Join taxi trips to hourly weather readings at the time of pickup

with trips as (
  -- pull all cleaned taxi trips from staging
  select * from {{ ref('stg_yellow_taxi') }}
),

weather as (
  -- pull all cleaned hourly weather from staging
  select * from {{ ref('stg_weather') }}
),

enriched as (
  select
    -- trip time dimensions
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_date,
    t.pickup_hour,
    t.pickup_day_of_week,
    t.is_weekend,

    -- trip location identifiers
    t.pickup_location_id,
    t.dropoff_location_id,

    -- trip characteristics
    t.vendor_id,
    t.passenger_count,
    t.trip_distance_miles,
    t.trip_duration_minutes,
    t.rate_code_id,

    -- trip fare breakdown
    t.payment_type,
    t.fare_amount,
    t.extra_charge,
    t.tip_amount,
    t.tolls_amount,
    t.airport_fee,
    t.total_amount,
    t.cbd_congestion_fee,
    t.congestion_surcharge,
    t.improvement_surcharge,
    t.mta_tax,

    -- weather time identifiers
    w.weather_datetime,
    w.weather_date,
    w.weather_hour,

    -- weather conditions at the moment of pickup
    w.temperature_celsius,
    w.temperature_fahrenheit,
    w.precipitation_mm,
    w.snowfall_cm,
    w.windspeed_kmh,
    w.visibility_metres,
    w.weather_severity,
    w.has_precipitation,
    w.has_snowfall,
    w.is_below_freezing

  from trips t
  left join weather w
    on t.pickup_date = w.weather_date
    and t.pickup_hour = w.weather_hour
)

select * from enriched