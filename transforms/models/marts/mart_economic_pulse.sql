-- Mart: The Economic Pulse Score
-- One row per month. Combines normalised signals from all sources
-- into a single composite index (0–100) where higher = more affordability pressure.
--
-- Component signals (each normalised 0–1 within its historical range):
--   1. food_price_index     — national median food price, normalised
--   2. fuel_cost_index      — RON95 price, normalised
--   3. cpi_index            — overall CPI, normalised
--   4. transport_index      — rail ridership growth (inverse: rising = less car pressure)
--   5. ev_adoption_index    — EV share of new registrations (inverse signal)
--
-- Score = weighted average of components × 100
--
-- Weights are grounded in the DOSM Household Expenditure Survey 2024
-- (mean monthly spending RM5,566; source: dosm.gov.my HES 2024):
--   food_norm:      35% — food at home (15.7%) + dining out (17.0%) = 32.7% of HES
--   cpi_norm:       30% — broad catch-all covering housing/utilities (23.5%) + health/education/etc.
--   fuel_norm:      15% — RON95 is one component of transport (11% of HES); not the full budget
--   transport_norm: 10% — transport share of HES ≈ 11%
--   ev_norm:        10% — forward-looking signal; kept as minor modifier

with

-- 1. Food price signal — national median across all food categories
food as (
    select
        month,
        median(median_price) as national_food_median
    from {{ ref('mart_price_summary') }}
    where item_category in (
        'AYAM','TELUR','BERAS','MINYAK DAN LEMAK',
        'BAWANG','SAYUR-SAYURAN','BAHAN LAUT','IKAN DARAT','BUAH-BUAHAN'
    )
    group by 1
),

-- 2. Fuel signal — RON95 price per month (use latest weekly price in that month)
fuel as (
    select
        month,
        last(price order by fuel_date) as ron95_price
    from {{ ref('mart_fuel_timeline') }}
    where fuel_type = 'RON95'
    group by 1
),

-- 3. CPI signal — national all-items CPI (use 'All' or equivalent division)
cpi as (
    select
        month,
        avg(cpi_value) as avg_cpi
    from {{ ref('stg_cpi') }}
    group by 1
),

-- 4. Transport signal — monthly total ridership (all services)
transport as (
    select
        month,
        sum(ridership) as total_ridership
    from {{ ref('stg_transport') }}
    group by 1
),

-- 5. EV adoption signal — EV share of total vehicle registrations
ev as (
    select
        month,
        sum(case when lower(fuel_type) in ('electric','ev','bev','phev') then registrations else 0 end)
            * 1.0 / nullif(sum(registrations), 0) as ev_share
    from {{ ref('stg_vehicles') }}
    group by 1
),

-- Join all signals on month
combined as (
    select
        coalesce(food.month, fuel.month, cpi.month) as month,
        food.national_food_median,
        fuel.ron95_price,
        cpi.avg_cpi,
        transport.total_ridership,
        ev.ev_share
    from food
    full outer join fuel      using (month)
    full outer join cpi       using (month)
    full outer join transport using (month)
    full outer join ev        using (month)
    where coalesce(food.month, fuel.month, cpi.month) is not null
),

-- Normalise each signal 0–1 using min-max over the full history
normalised as (
    select
        month,
        national_food_median,
        ron95_price,
        avg_cpi,
        total_ridership,
        ev_share,

        -- Price/fuel/CPI: higher = more pressure (0=cheapest ever, 1=most expensive ever)
        (national_food_median - min(national_food_median) over ())
            / nullif(max(national_food_median) over () - min(national_food_median) over (), 0)
            as food_norm,

        (ron95_price - min(ron95_price) over ())
            / nullif(max(ron95_price) over () - min(ron95_price) over (), 0)
            as fuel_norm,

        (avg_cpi - min(avg_cpi) over ())
            / nullif(max(avg_cpi) over () - min(avg_cpi) over (), 0)
            as cpi_norm,

        -- Ridership: higher ridership = LESS pressure (people using PT instead of driving)
        1 - (total_ridership - min(total_ridership) over ())
            / nullif(max(total_ridership) over () - min(total_ridership) over (), 0)
            as transport_pressure_norm,

        -- EV share: higher = LESS pressure
        1 - (ev_share - min(ev_share) over ())
            / nullif(max(ev_share) over () - min(ev_share) over (), 0)
            as ev_pressure_norm
    from combined
)

select
    month,
    national_food_median,
    ron95_price,
    avg_cpi,
    total_ridership,
    ev_share,

    -- Weighted composite score (weights sum to 1.0)
    -- Weights derived from DOSM Household Expenditure Survey 2024
    round(
        (
            coalesce(food_norm, 0.5)             * 0.35 +   -- food prices: 35% (food@home 15.7% + dining 17.0% = 32.7% of HES)
            coalesce(cpi_norm, 0.5)              * 0.30 +   -- CPI:         30% (covers housing/utilities 23.5% + health/edu/etc.)
            coalesce(fuel_norm, 0.5)             * 0.15 +   -- fuel cost:   15% (RON95 ⊂ transport 11% of HES)
            coalesce(transport_pressure_norm,0.5)* 0.10 +   -- transport:   10% (transport ≈ 11% of HES)
            coalesce(ev_pressure_norm, 0.5)      * 0.10     -- EV adoption: 10% (forward-looking modifier)
        ) * 100
    , 1)                                        as pulse_score,

    -- Readable pressure label
    case
        when (coalesce(food_norm,0.5)*0.35 + coalesce(cpi_norm,0.5)*0.30 +
              coalesce(fuel_norm,0.5)*0.15 + coalesce(transport_pressure_norm,0.5)*0.10 +
              coalesce(ev_pressure_norm,0.5)*0.10) < 0.33  then 'Low Pressure'
        when (coalesce(food_norm,0.5)*0.35 + coalesce(cpi_norm,0.5)*0.30 +
              coalesce(fuel_norm,0.5)*0.15 + coalesce(transport_pressure_norm,0.5)*0.10 +
              coalesce(ev_pressure_norm,0.5)*0.10) < 0.66  then 'Moderate Pressure'
        else                                                     'High Pressure'
    end                                         as pressure_label

from normalised
order by month
