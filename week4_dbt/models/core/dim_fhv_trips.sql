{{
    config(
        materialized='table'
    )
}}
with fhv_tripdata as (
    select * 
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripdata.dispatching_base_num,
fhv_tripdata.pickup_datetime, 
fhv_tripdata.dropoff_datetime AS dropoff_datetime,
fhv_tripdata.pulocationid AS pickup_location_id,
fhv_tripdata.dolocationid AS dropoff_location_id,
fhv_tripdata.sr_flag,
fhv_tripdata.affiliated_base_number,
pickup_zone.zone AS pickup_zones,
dropoff_zone.zone AS dropoff_zones
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dolocationid = dropoff_zone.locationid