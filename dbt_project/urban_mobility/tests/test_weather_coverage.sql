-- Test: every trip should have weather data
-- NULL weather_severity means the weather join failed for that hour

select
    trip_id,
    pickup_datetime,
    weather_severity
from {{ ref('fact_trips') }}
where weather_severity is null