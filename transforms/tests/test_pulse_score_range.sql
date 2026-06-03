-- Pulse score must always be between 0 and 100
select * from {{ ref('mart_economic_pulse') }}
where pulse_score < 0 or pulse_score > 100
