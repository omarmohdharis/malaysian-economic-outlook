-- Staging: Fuel prices (weekly, long format)
-- Source is wide format (series_type, date, ron95, ron97, diesel, ...).
-- CDC layer has already normalised this to long format in warehouse.fuel_price_history.

with raw as (
    select * from warehouse.fuel_price_history
)

select
    date_effective                    as fuel_date,
    strftime(date_effective, '%Y-%m') as month,
    strftime(date_effective, '%Y')    as year,
    upper(trim(fuel_type))            as fuel_type,
    price
from raw
where price is not null
  and price > 0
