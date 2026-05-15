-- Test: all pickup and dropoff location IDs must exist in dim_location
-- Orphaned location IDs would break dashboard zone-level analysis

select
    f.trip_id,
    f.pickup_location_id
from {{ ref('fact_trips') }} f
left join {{ ref('dim_location') }} d
    on f.pickup_location_id = d.location_id
where d.location_id is null