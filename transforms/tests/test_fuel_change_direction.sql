-- change_direction must always be one of the four valid values
select * from {{ ref('mart_fuel_timeline') }}
where change_direction not in ('UP', 'DOWN', 'UNCHANGED', 'FIRST')
