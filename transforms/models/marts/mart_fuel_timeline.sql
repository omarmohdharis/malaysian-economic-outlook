-- Mart: Fuel price timeline with change annotations
-- Every row is one week × fuel type.
-- Includes direction of change vs prior week for easy dashboard use.

with fuel as (
    select * from {{ ref('stg_fuel') }}
),

with_prev as (
    select
        *,
        lag(price) over (
            partition by fuel_type
            order by fuel_date
        ) as prev_price
    from fuel
),

annotated as (
    select
        fuel_date,
        month,
        year,
        fuel_type,
        price,
        prev_price,
        round(price - coalesce(prev_price, price), 3)           as price_change,
        case
            when prev_price is null                 then 'FIRST'
            when price > prev_price                 then 'UP'
            when price < prev_price                 then 'DOWN'
            else                                         'UNCHANGED'
        end                                                     as change_direction,
        case
            when prev_price is not null and prev_price > 0
            then round((price - prev_price) / prev_price * 100, 3)
        end                                                     as change_pct
    from with_prev
)

select * from annotated
order by fuel_type, fuel_date
