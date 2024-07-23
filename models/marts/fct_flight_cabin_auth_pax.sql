WITH authorized_pax AS (
    SELECT * FROM {{ ref('int_authorized_pax_qty') }}
)

SELECT
    airline_code,
    flight_number,
    departure_date,
    departure_airport,
    flight_key,
    'F' AS cabin_class,
    first_class_authorized_pax AS authorized_pax_qty
FROM authorized_pax

UNION ALL

SELECT
    airline_code,
    flight_number,
    departure_date,
    departure_airport,
    flight_key,
    'C' AS cabin_class,
    business_class_authorized_pax AS authorized_pax_qty
FROM authorized_pax

UNION ALL

SELECT
    airline_code,
    flight_number,
    departure_date,
    departure_airport,
    flight_key,
    'W' AS cabin_class,
    premium_economy_authorized_pax AS authorized_pax_qty
FROM authorized_pax

UNION ALL

SELECT
    airline_code,
    flight_number,
    departure_date,
    departure_airport,
    flight_key,
    'Y' AS cabin_class,
    economy_authorized_pax AS authorized_pax_qty
FROM authorized_pax