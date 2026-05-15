with zones as (
  select * from {{ ref('taxi_zone')}}
),

final as(
  select 
    cast(LocationID	as int64)   as location_id,
    Borough                     as borough,
    Zone                        as zone_name,	
    service_zone                as service_zone,

    case Borough
      when 'Manhattan'      then 1
      when 'Queens'         then 2
      when 'Brooklyn'       then 3
      when 'Bronx'          then 4
      when 'Staten Island'  then 5
      when 'EWR'            then 6
      when 'Unknown'        then 7
      when 'N/A'            then 8
      else 99
  end                           as borough_rank,

    case service_zone
      when 'Yellow Zone'     then 'high_demand'
      when 'Boro Zone'       then 'medium_demand'
      when 'Airports'        then 'airport'
      when 'EWR'             then 'airport'
      when 'N/A'             then 'unassigned'
      else 'other'
  end                           as demand_category

  from zones
)

select * from final