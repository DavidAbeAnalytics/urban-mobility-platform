{{ config(severity='warn') }}

-- Data quality observation: physically impossible speed records
-- 287 trips show over 100 miles in under 30 minutes - GPS or meter errors
-- in TLC source data. Flagged for monitoring, not blocking.

select
    trip_id,
    trip_distance_miles,
    trip_duration_minutes,
    round(trip_distance_miles / nullif(trip_duration_minutes, 0) * 60, 1)
        as implied_speed_mph
from {{ ref('fact_trips') }}
where trip_distance_miles > 100
and trip_duration_minutes < 30