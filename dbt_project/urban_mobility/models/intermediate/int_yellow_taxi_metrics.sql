-- add analytical buckets, flags, and derived metrics on top of weather-enriched trips

with base as (
  select * from {{ ref('int_yellow_taxi_weather')}}
),

metrics as(
  select 
    * ,

    -- trip duration bucket: convert minutes into readable category
    case
      when trip_duration_minutes < 10                                    then 'short'
      when trip_duration_minutes >= 10 and trip_duration_minutes < 30    then 'medium'
      when trip_duration_minutes >= 30 and trip_duration_minutes < 60    then 'long'
      else                                                                    'very_long'
    end as trip_duration_bucket,

    -- distance bucket: converts miles into a readable category
    case
      when trip_distance_miles < 1                                        then 'under_1_mile'
      when trip_distance_miles >= 1  and trip_distance_miles < 6          then '1_to_5_miles'
      when trip_distance_miles >= 6  and trip_distance_miles < 21         then '6_to_20_miles'
      when trip_distance_miles >= 21 and trip_distance_miles < 51         then '21_to_50_miles'
      when trip_distance_miles >= 51 and trip_distance_miles <= 100       then '51_to_100_miles'
      else                                                                     'over_100_miles'
    end as trip_distance_bucket,

    -- time of day label: classifies each trip by when it happened
    case
      when pickup_hour between 4  and 8                                    then 'early_morning'
      when pickup_hour between 9  and 11                                   then 'late_morning'
      when pickup_hour between 12 and 16                                   then 'afternoon_peak'
      when pickup_hour between 17 and 19                                   then 'evening_peak'
      when pickup_hour between 20 and 23                                   then 'late_night'
      when pickup_hour between 0  and 3                                    then 'mid_night'
      else                                                                      'off_peak'
    end as time_of_day,    

    -- Boolean flag: true if the trip started during a rush hour window
    case
      when pickup_hour between 7  and 9                                     then true
      when pickup_hour between 17 and 19                                    then true
      else                                                                       false
    end as is_peak_hour,

    -- Tip as a percentage of the base fare
    round( safe_divide(tip_amount, fare_amount) * 100, 2 )                   as tip_percentage,

    
    -- Estimated average speed in miles per hour
    -- distance / duration gives miles-per-minute, multiply by 60 for mph
    round( safe_divide(trip_distance_miles, trip_duration_minutes) * 60, 2 ) as avg_speed_mph,


    -- Weather flags: make snow/rain filtering a simple boolean in the marts layer
    case
      when weather_severity in ('heavy_snow', 'light_snow')                   then true
      else false
    end as is_snow_trip,

    case
      when weather_severity in ('heavy_rain', 'light_rain')                    then true
      else                                                                          false
    end as is_rain_trip

  from base
)

select * from metrics