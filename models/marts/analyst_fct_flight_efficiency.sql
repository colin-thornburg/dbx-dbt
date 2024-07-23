WITH flight_delays AS (
    SELECT * FROM {{ ref('stg_analyst_flight_delays') }}
),

flight_cabin_auth_pax AS (
    SELECT * FROM {{ ref('fct_flight_cabin_auth_pax') }}
)

SELECT
    fd.flight_key,
    fd.delay_minutes,
    CASE
        WHEN fd.delay_minutes <= 15 THEN 'Short Delay'
        WHEN fd.delay_minutes <= 30 THEN 'Medium Delay'
        ELSE 'Long Delay'
    END AS delay_category,
    fcap.cabin_class,
    fcap.authorized_pax_qty,
    fcap.airline_code,
    fcap.flight_number,
    fcap.departure_date,
    fcap.departure_airport
FROM flight_delays fd
LEFT JOIN flight_cabin_auth_pax fcap ON fd.flight_key = fcap.flight_key