with trips as (
  select * from {{ ref('int_yellow_taxi_metrics') }}
),

subway_impact as (
  select * from {{ ref('int_subway_impact') }}
),

locations as (
  select * from {{ ref('dim_location') }}
),

time_dim as (
  select * from {{ ref('dim_time') }}
),

joined as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'trips.pickup_datetime',
      'trips.dropoff_datetime',
      'trips.pickup_location_id',
      'trips.dropoff_location_id',
      'trips.vendor_id',
      'trips.fare_amount',
      'trips.trip_distance_miles',
      'trips.passenger_count'
    ]) }}                                       as trip_id,

    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.pickup_date,
    trips.pickup_hour,
    trips.pickup_day_of_week,
    trips.is_weekend,
    t.time_of_day,
    t.is_holiday_period,
    t.analysis_month,

    trips.pickup_location_id,
    trips.dropoff_location_id,
    pl.borough                                  as pickup_borough,
    pl.zone_name                                as pickup_zone,
    pl.demand_category                          as pickup_demand_category,
    dl.borough                                  as dropoff_borough,
    dl.zone_name                                as dropoff_zone,

    trips.passenger_count,
    trips.trip_distance_miles,
    trips.trip_duration_minutes,
    trips.trip_duration_bucket,
    trips.trip_distance_bucket,
    trips.avg_speed_mph,
    trips.rate_code_id,
    trips.payment_type,

    trips.fare_amount,
    trips.tip_amount,
    trips.tolls_amount,
    trips.total_amount,
    trips.tip_percentage,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.mta_tax,

    trips.temperature_celsius,
    trips.precipitation_mm,
    trips.snowfall_cm,
    trips.weather_severity,
    trips.is_snow_trip,
    trips.is_rain_trip,
    trips.is_below_freezing,

    coalesce(si.has_subway_disruption, false)   as has_subway_disruption,
    coalesce(si.subway_lines_affected, 0)       as subway_lines_affected,
    coalesce(si.max_disruption_severity, 0)     as max_disruption_severity,
    coalesce(si.is_compound_disruption, false)  as is_compound_disruption,
    coalesce(si.has_synthetic_mta_data, false)  as has_synthetic_mta_data,

    trips.vendor_id

  from trips

  left join subway_impact si
    on trips.pickup_date = si.pickup_date
    and trips.pickup_hour = si.pickup_hour
    and trips.pickup_location_id = si.pickup_location_id

  left join locations pl
    on trips.pickup_location_id = pl.location_id

  left join locations dl
    on trips.dropoff_location_id = dl.location_id

  left join time_dim t
    on timestamp_trunc(trips.pickup_datetime, hour) = t.datetime_hour  
),

deduped as (
  select *
  from 
    (
      select *,
        row_number() over (partition by trip_id order by pickup_datetime) as row_num
      from joined
    )
  where row_num = 1
)

select
  trip_id,
  pickup_datetime,
  dropoff_datetime,
  pickup_date,
  pickup_hour,
  pickup_day_of_week,
  is_weekend,
  time_of_day,
  is_holiday_period,
  analysis_month,
  pickup_location_id,
  dropoff_location_id,
  pickup_borough,
  pickup_zone,
  pickup_demand_category,
  dropoff_borough,
  dropoff_zone,
  passenger_count,
  trip_distance_miles,
  trip_duration_minutes,
  trip_duration_bucket,
  trip_distance_bucket,
  avg_speed_mph,
  rate_code_id,
  payment_type,
  fare_amount,
  tip_amount,
  tolls_amount,
  total_amount,
  tip_percentage,
  congestion_surcharge,
  airport_fee,
  mta_tax,
  temperature_celsius,
  precipitation_mm,
  snowfall_cm,
  weather_severity,
  is_snow_trip,
  is_rain_trip,
  is_below_freezing,
  has_subway_disruption,
  subway_lines_affected,
  max_disruption_severity,
  is_compound_disruption,
  has_synthetic_mta_data,
  vendor_id
from deduped