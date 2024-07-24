{{ config(
    materialized='incremental',
    unique_key=['flight_key', 'cabin_class'],
    on_schema_change='fail'
) }}

WITH authorized_pax AS (
    SELECT 
        airline_code,
        flight_number,
        departure_date,
        departure_airport,
        flight_key,
        first_class_authorized_pax,
        business_class_authorized_pax,
        premium_economy_authorized_pax,
        economy_authorized_pax,
        sabre_transaction_timestamp
    FROM {{ ref('int_authorized_pax_qty') }}
    {% if is_incremental() %}
    WHERE sabre_transaction_timestamp > (SELECT COALESCE(MAX(sabre_transaction_timestamp), '1900-01-01') FROM {{ this }})
    {% endif %}
),

unpivoted AS (
    SELECT
        airline_code,
        flight_number,
        departure_date,
        departure_airport,
        flight_key,
        'F' as cabin_class,
        first_class_authorized_pax as authorized_pax_qty,
        sabre_transaction_timestamp
    FROM authorized_pax
    WHERE first_class_authorized_pax IS NOT NULL

    UNION ALL

    SELECT
        airline_code,
        flight_number,
        departure_date,
        departure_airport,
        flight_key,
        'C' as cabin_class,
        business_class_authorized_pax as authorized_pax_qty,
        sabre_transaction_timestamp
    FROM authorized_pax
    WHERE business_class_authorized_pax IS NOT NULL

    UNION ALL

    SELECT
        airline_code,
        flight_number,
        departure_date,
        departure_airport,
        flight_key,
        'W' as cabin_class,
        premium_economy_authorized_pax as authorized_pax_qty,
        sabre_transaction_timestamp
    FROM authorized_pax
    WHERE premium_economy_authorized_pax IS NOT NULL

    UNION ALL

    SELECT
        airline_code,
        flight_number,
        departure_date,
        departure_airport,
        flight_key,
        'Y' as cabin_class,
        economy_authorized_pax as authorized_pax_qty,
        sabre_transaction_timestamp
    FROM authorized_pax
    WHERE economy_authorized_pax IS NOT NULL
)

SELECT
    airline_code,
    flight_number,
    departure_date,
    departure_airport,
    flight_key,
    cabin_class,
    authorized_pax_qty,
    sabre_transaction_timestamp
FROM unpivoted