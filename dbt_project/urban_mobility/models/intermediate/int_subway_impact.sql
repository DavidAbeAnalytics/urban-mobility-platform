-- Join zone-hour taxi demand to subway disruption events

with demand as (
  -- Zone-hour aggregated taxi demand
  select * from {{ ref('int_demand_zone_hour') }}
),

alerts as (
  -- Cleaned MTA subway disruption alerts from staging
  select * from {{ ref('stg_mta_alerts') }}
),

alert_hours as (
  -- Extract the start and end hour from each alert's timestamps
  select
    alert_date,
    subway_line,
    cause,
    effect,
    disruption_severity_score,
    is_synthetic,
    extract(hour from alert_start_datetime)     as alert_start_hour,
    extract(hour from alert_end_datetime)       as alert_end_hour,
    alert_duration_minutes
  from alerts
),

hourly_disruptions as (
  select
    alert_date,
    hour_of_day,

    -- Count alerts at different severity thresholds for this date-hour
    countif(disruption_severity_score >= 4)     as severe_alert_count,
    countif(disruption_severity_score >= 2)     as moderate_alert_count,

    -- How many distinct subway lines were disrupted this hour
    count(distinct subway_line)                 as lines_affected,

    -- The worst single disruption score in this hour
    max(disruption_severity_score)              as max_severity_score,

    -- Flag if any of the alert data in this hour was synthetically generated
    logical_or(is_synthetic)                    as has_synthetic_data

    from alert_hours
    -- generate_array creates a list of integers from start_hour to end_hour
    -- e.g. an alert from hour 8 to hour 11 becomes [8, 9, 10, 11]
    -- unnest then expands that array into individual rows, one per hour
    -- This means a 4-hour alert contributes to 4 separate hour buckets
    cross join unnest(
      generate_array(alert_start_hour, least(alert_end_hour, 23))
    ) as hour_of_day
    group by alert_date, hour_of_day
),

impact as (
  select
    -- Core demand dimensions
    d.pickup_date,
    d.pickup_hour,
    d.pickup_location_id,

    -- Key demand metrics
    d.trip_count,
    d.avg_fare,
    d.avg_duration_minutes,
    d.avg_speed_mph,
    d.total_fare_revenue,

    -- Weather context
    d.weather_severity,
    d.temperature_celsius,
    d.snowfall_cm,
    d.precipitation_mm,

    -- Subway disruption metrics for this date-hour
    -- coalesce replaces NULL (no disruption recorded) with 0
    coalesce(h.severe_alert_count, 0)           as severe_subway_alerts,
    coalesce(h.moderate_alert_count, 0)         as moderate_subway_alerts,
    coalesce(h.lines_affected, 0)               as subway_lines_affected,
    coalesce(h.max_severity_score, 0)           as max_disruption_severity,
    coalesce(h.has_synthetic_data, false)       as has_synthetic_mta_data,

    -- Simple boolean: was there any severe disruption this hour?
    case
      when h.severe_alert_count > 0 then true
      else false
    end                                         as has_subway_disruption,

    -- Compound flag: severe disruption + bad weather simultaneously
    -- The most analytically interesting scenario for demand spike analysis
    case
      when h.severe_alert_count > 0
        and d.weather_severity in ('heavy_snow', 'light_snow', 'heavy_rain')
      then true
      else false
    end                                         as is_compound_disruption

  from demand d
  -- Left join so every zone-hour is kept even if no alerts exist for that hour
  left join hourly_disruptions h
    on d.pickup_date = h.alert_date
    and d.pickup_hour = h.hour_of_day
)

select * from impact