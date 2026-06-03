-- Staging: Daily public transport ridership
-- Source is wide format — one column per service. Unpivot to long format here.

with raw as (
    select * from raw.transport_ridership
),

unpivoted as (
    unpivot raw
    on bus_rkl, bus_rkn, bus_rpn,
       rail_lrt_ampang, rail_lrt_kj, rail_monorail,
       rail_mrt_kajang, rail_mrt_pjy,
       rail_ets, rail_intercity, rail_komuter, rail_komuter_utara, rail_tebrau
    into
        name  service
        value ridership
)

select
    date::date                    as ride_date,
    strftime(date::date, '%Y-%m') as month,
    service,
    cast(ridership as integer)    as ridership
from unpivoted
where ridership is not null
  and ridership > 0
