-- All prices must be positive
select * from {{ ref('stg_prices') }}
where price <= 0
