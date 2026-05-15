-- Pre-aggregate trip demand by pickup zone, date, and hour
-- Reduces 12M trip rows to a few thousand zone-hour summary rows

with trips as (
  select * from {{ ref('int_yellow_taxi_metrics') }}
),

aggregated as (
  select 
    -- Grouping dimensions: the "who / when / where" of each summary row
    pickup_date,
    pickup_hour,
    pickup_location_id,
    is_weekend,
    time_of_day,

    -- Weather conditions carried through at the zone-hour level
    weather_severity,
    has_snowfall,
    has_precipitation,
    is_below_freezing,
    temperature_celsius,
    precipitation_mm,
    snowfall_cm,

    -- Volume and financial aggregates
    count(*)                                        as trip_count,
    round(avg(fare_amount), 2)                      as avg_fare,
    round(avg(total_amount), 2)                     as avg_total_amount,

    -- Performance aggregates
    round(avg(trip_duration_minutes), 1)            as avg_duration_minutes,
    round(avg(trip_distance_miles), 2)              as avg_distance_miles,
    round(avg(tip_percentage), 2)                   as avg_tip_percentage,
    round(avg(avg_speed_mph), 2)                    as avg_speed_mph,

    -- Total revenue for this zone-hour window
    sum(fare_amount)                                as total_fare_revenue,

    -- Conditional counts using countif (BigQuery-native)
    countif(is_peak_hour)                           as peak_hour_trips,
    countif(is_snow_trip)                           as snow_trip_count,
    countif(is_rain_trip)                           as rain_trip_count,

    -- Payment method split for this zone-hour
    countif(payment_type = 1)                       as card_payment_count,
    countif(payment_type = 2)                       as cash_payment_count

  from trips
  group by
    pickup_date,
    pickup_hour,
    pickup_location_id,
    is_weekend,
    time_of_day,
    weather_severity,
    has_snowfall,
    has_precipitation,
    is_below_freezing,
    temperature_celsius,
    precipitation_mm,
    snowfall_cm
)

select * from aggregated