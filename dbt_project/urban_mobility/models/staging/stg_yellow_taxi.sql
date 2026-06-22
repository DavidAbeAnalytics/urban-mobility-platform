with source as (
    select * from {{ source('raw', 'yellow_taxi_raw') }}
),

deduped as (
    select *,
        row_number() over (
            partition by
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                VendorID,
                cast(fare_amount * 100 as int64),
                cast(trip_distance * 100 as int64),
                passenger_count
            order by tpep_pickup_datetime
        ) as row_num
    from source
),

renamed as (
    select
        VendorID                                    as vendor_id,
        cast(tpep_pickup_datetime as timestamp)     as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp)    as dropoff_datetime,
        cast(PULocationID as int64)                 as pickup_location_id,
        cast(DOLocationID as int64)                 as dropoff_location_id,

        -- trip details
        cast(passenger_count as int64)              as passenger_count,
        cast(trip_distance as float64)              as trip_distance_miles,
        cast(RatecodeID as int64)                   as rate_code_id,
        store_and_fwd_flag                          as store_and_forward_flag,

        -- payments
        cast(payment_type as int64)                 as payment_type,
        cast(fare_amount as float64)                as fare_amount,
        cast(extra as float64)                      as extra_charge,
        cast(mta_tax as float64)                    as mta_tax,
        cast(tip_amount as float64)                 as tip_amount,
        cast(tolls_amount as float64)               as tolls_amount,
        cast(improvement_surcharge as float64)      as improvement_surcharge,
        cast(congestion_surcharge as float64)       as congestion_surcharge,
        cast(Airport_fee as float64)                as airport_fee,
        cast(cbd_congestion_fee as float64)         as cbd_congestion_fee,
        cast(total_amount as float64)               as total_amount,

        -- Derived column: calculated once here to avoid redundant logic in downstream models
        timestamp_diff(
            cast(tpep_dropoff_datetime as timestamp),
            cast(tpep_pickup_datetime as timestamp),
            minute
        )                                           as trip_duration_minutes,

        date(tpep_pickup_datetime)                  as pickup_date,
        extract(hour from tpep_pickup_datetime)     as pickup_hour,
        extract(dayofweek from tpep_pickup_datetime) as pickup_day_of_week,
        case
            when extract(dayofweek from tpep_pickup_datetime) in (1, 7) then true
            else false
        end                                         as is_weekend

    from deduped
    where row_num = 1
),

filtered as (
    select *
    from renamed
    where
        pickup_datetime >= '2025-11-01'
        and pickup_datetime < '2026-02-01'
        and trip_duration_minutes > 0
        and trip_duration_minutes <= 90 
        and trip_distance_miles > 0
        and trip_distance_miles <= 40          -- max 40 miles for NYC trips
        and fare_amount > 0
        and fare_amount <= 150                 -- remove extreme fare outliers
        and pickup_location_id is not null
        and dropoff_location_id is not null
)

select * from filtered