with source as (
  select * from {{ source('raw', 'mta_alerts_raw') }}
),

cleaned as (
  select
    alert_id,
    subway_line,
    timestamp_seconds( cast(start_time as int64) )                as alert_start_datetime,
    timestamp_seconds( cast(end_time as int64) )                  as alert_end_datetime,
    
    -- alert durations
    timestamp_diff(
      timestamp_seconds(cast(end_time as int64)),
      timestamp_seconds(cast(start_time as int64)),
      minute )                                                    as alert_duration_minutes,
    
    -- alert classifications
    cause,
    effect,

    -- severity score for ranking disruptions
    case effect
      when 'NO_SERVICE'           then 5
      when 'SIGNIFICANT_DELAYS'   then 4
      when 'REDUCED_SERVICE'      then 3
      when 'MODIFIED_SERVICE'     then 2
      when 'DETOUR'               then 1
      else 0
    end                                                           as disruption_severity_score,
    
    -- data quality flags
    cast(is_synthetic as bool)                                    as is_synthetic,
    ingested_at,
    
    -- date helpers for joining to trips
    date(timestamp_seconds( cast(start_time as int64) ))          as alert_date
  
  from source
  where
      start_time is not null
      and end_time is not null
      and start_time != ''
      and end_time != ''
)

select * from cleaned