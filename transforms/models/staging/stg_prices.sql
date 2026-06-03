-- Staging: PriceCatcher prices
-- Joins raw price records to item and premise lookup tables.
-- Filters to valid prices only and normalises text fields.

with raw_prices as (
    select * from raw.pricecatcher_prices
),

items as (
    select
        item_code,
        item,
        unit,
        item_group,
        item_category
    from raw.pricecatcher_items
    where item_code is not null
      and item      is not null
),

premises as (
    select
        premise_code,
        premise,
        premise_type,
        state,
        district
    from raw.pricecatcher_premises
    where premise_code is not null
      and state        is not null
),

joined as (
    select
        p.date::date                                    as price_date,
        p._month_partition                              as month,
        cast(p.item_code as integer)                    as item_code,
        trim(regexp_replace(i.item,         '\s+', ' ')) as item_name,
        trim(regexp_replace(i.unit,         '\s+', ' ')) as unit,
        trim(regexp_replace(i.item_group,   '\s+', ' ')) as item_group,
        trim(regexp_replace(i.item_category,'\s+', ' ')) as item_category,
        cast(p.premise_code as integer)                 as premise_code,
        trim(regexp_replace(pr.premise,      '\s+', ' ')) as premise_name,
        trim(regexp_replace(pr.premise_type, '\s+', ' ')) as premise_type,
        trim(regexp_replace(pr.state,        '\s+', ' ')) as state,
        trim(regexp_replace(pr.district,     '\s+', ' ')) as district,
        cast(p.price as double)                         as price
    from raw_prices p
    left join items    i  on cast(p.item_code   as integer) = i.item_code
    left join premises pr on cast(p.premise_code as integer) = pr.premise_code
    where cast(p.price as double) > 0
      and i.item_code is not null
      and pr.premise_code is not null
)

select * from joined
