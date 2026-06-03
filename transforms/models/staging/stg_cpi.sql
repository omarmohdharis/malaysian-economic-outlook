-- Staging: CPI by state and division
-- Normalises column names and ensures date typing.

with raw as (
    select * from raw.cpi_state
)

select
    date::date                    as cpi_date,
    strftime(date::date, '%Y-%m') as month,
    trim(state)                   as state,
    trim(division)                as division,   -- '01'–'13' or 'overall'
    cast(index as double)         as cpi_value
from raw
where date  is not null
  and index is not null
