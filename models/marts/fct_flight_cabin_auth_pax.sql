{{ config(
    materialized='incremental',
    unique_key=['flight_key', 'cabin_class'],
    on_schema_change='fail'
) }}

{% if is_incremental() %}

WITH max_timestamp AS (
    SELECT COALESCE(MAX(sabre_transaction_timestamp), '1900-01-01') AS max_timestamp
    FROM {{ this }}
),

authorized_pax AS (
    SELECT a.*
    FROM {{ ref('int_authorized_pax_qty') }} a
    CROSS JOIN max_timestamp
    WHERE a.sabre_transaction_timestamp > max_timestamp.max_timestamp
)

{% else %}

WITH authorized_pax AS (
    SELECT * FROM {{ ref('int_authorized_pax_qty') }}
)

{% endif %}

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