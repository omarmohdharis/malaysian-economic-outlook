-- Staging: Quarterly GDP
-- Source columns: series (abs/growth_yoy/growth_qoq), date, value
-- CDC layer stores this in warehouse.gdp_quarterly as quarter, series, value.

with raw as (
    select * from warehouse.gdp_quarterly
)

select
    quarter,
    trim(series)        as series_type,   -- 'abs', 'growth_yoy', 'growth_qoq'
    value               as gdp_value,
    loaded_at
from raw
where value is not null
