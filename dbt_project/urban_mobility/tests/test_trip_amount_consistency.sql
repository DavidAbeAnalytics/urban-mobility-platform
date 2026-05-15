{{ config(severity='warn') }}

-- Data quality observation: total_amount < fare_amount
-- Found in TLC source data due to legitimate negative adjustments
-- and dispute credits applied by vendors. Flagged for monitoring,
-- not treated as a blocking error. See ADR-009.

select
    trip_id,
    fare_amount,
    total_amount,
    round(fare_amount - total_amount, 2) as adjustment_amount
from {{ ref('fact_trips') }}
where total_amount < fare_amount