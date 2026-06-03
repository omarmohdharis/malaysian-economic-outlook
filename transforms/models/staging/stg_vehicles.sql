-- Staging: Monthly vehicle registrations by type and fuel type

with raw as (
    select * from raw.vehicle_registrations
)

select
    date::date                              as reg_date,
    strftime(date::date, '%Y-%m')           as month,
    trim(type)                              as vehicle_type,
    trim(fuel)                              as fuel_type,
    cast(registrations as integer)          as registrations
from raw
where registrations is not null
  and registrations > 0
