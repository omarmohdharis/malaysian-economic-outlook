-- Mart: Monthly price summary by state and category
-- This is the primary fact table for price analysis.
-- Pre-aggregates PriceCatcher records to median/mean/spread per month×state×category.

with prices as (
    select * from {{ ref('stg_prices') }}
),

monthly_stats as (
    select
        month,
        state,
        item_category,
        count(*)                                                as sample_count,
        round(avg(price), 2)                                    as avg_price,
        round(median(price), 2)                                 as median_price,
        round(percentile_cont(0.25) within group (order by price), 2) as p25_price,
        round(percentile_cont(0.75) within group (order by price), 2) as p75_price,
        round(min(price), 2)                                    as min_price,
        round(max(price), 2)                                    as max_price,
        round(stddev(price), 2)                                 as price_stddev,
        count(distinct premise_code)                            as premise_count,
        count(distinct item_code)                               as item_count
    from prices
    group by 1, 2, 3
),

-- Add month-over-month change
with_mom as (
    select
        *,
        lag(median_price) over (
            partition by state, item_category
            order by month
        ) as prev_median_price
    from monthly_stats
)

select
    *,
    case
        when prev_median_price is not null and prev_median_price > 0
        then round((median_price - prev_median_price) / prev_median_price * 100, 2)
    end as mom_pct_change
from with_mom
order by month desc, state, item_category
