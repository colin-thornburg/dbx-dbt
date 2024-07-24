{{ config(
    materialized='incremental',
    unique_key=['flight_key', 'cabin_class'],
    on_schema_change='fail'
) }}

WITH authorized_pax AS (
    SELECT * 
    FROM {{ ref('int_authorized_pax_qty') }}
    {% if is_incremental() %}
    WHERE sabre_transaction_timestamp > (
        SELECT COALESCE(MAX(sabre_transaction_timestamp), '1900-01-01') 
        FROM {{ this }}
    )
    {% endif %}
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
FROM (
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
)
WHERE authorized_pax_qty IS NOT NULL