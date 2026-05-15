with date_spine as (
  {{ 
    dbt_utils.date_spine( datepart="hour", 
    start_date="cast('2025-11-01' as date)", 
    end_date="cast('2026-02-01' as date)" ) 
    }}
),

final as (
  select
    cast(date_hour as timestamp)                as datetime_hour,
    cast(date(date_hour) as date)               as date_day,
    extract(year from date_hour)                as year,
    extract(month from date_hour)               as month,
    extract(day from date_hour)                 as day,
    extract(hour from date_hour)                as hour,
    extract(dayofweek from date_hour)           as day_of_week,
    format_date('%A', date(date_hour))          as day_name,
    format_date('%B', date(date_hour))          as month_name,

  case
    when extract(dayofweek from date_hour) in (1, 7) then true
    else false
  end                                         as is_weekend,

  case
    when extract(hour from date_hour) between 5 and 8   then 'morning_peak'
    when extract(hour from date_hour) between 17 and 19 then 'evening_peak'
    when extract(hour from date_hour) between 20 and 23 then 'late_night'
    when extract(hour from date_hour) between 0 and 3   then 'mid_night'
    else 'off_peak'
  end                                         as time_of_day,

  case
    when extract(month from date_hour) = 11 then 'November'
    when extract(month from date_hour) = 12 then 'December'
    when extract(month from date_hour) = 1  then 'January'
  end                                         as analysis_month,

  case
    when extract(month from date_hour) = 12
      and extract(day from date_hour) between 20 and 31  then true
    when extract(month from date_hour) = 1
      and extract(day from date_hour) between 1 and 3    then true
    else false
  end                                         as is_holiday_period

  from date_spine
)

select * from final