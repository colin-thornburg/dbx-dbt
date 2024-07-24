WITH flight_core AS (
    SELECT * FROM {{ ref('stg_flight_core_hist') }}
),

jump_seats AS (
    SELECT * FROM {{ ref('stg_jump_seats') }}
),

broken_seats AS (
    SELECT * FROM {{ ref('stg_broken_seats') }}
),

combined_data AS (
    SELECT
        f.airline_code,
        f.flight_number,
        f.departure_date,
        f.departure_airport,
        f.flight_key,
        f.sabre_transaction_timestamp,
        f.gfa_first_class,
        f.gfa_business_class,
        f.gfa_premium_economy,
        f.gfa_economy,
        j.jump_seat_quantity,
        b.first_class_broken_seats,
        b.business_class_broken_seats,
        b.premium_economy_broken_seats,
        b.economy_broken_seats,
        b.first_class_broken_message_timestamp,
        b.business_class_broken_message_timestamp,
        b.premium_economy_broken_message_timestamp,
        b.economy_broken_message_timestamp
    FROM flight_core f
    LEFT JOIN jump_seats j ON f.flight_key = j.flight_key
    LEFT JOIN broken_seats b ON f.flight_key = b.flight_key
)

SELECT
    *,
    {{ first_class_authorized_pax() }} as first_class_authorized_pax,
     {{ business_class_authorized_pax() }} as business_class_authorized_pax,
    {{ premium_economy_authorized_pax() }} as premium_economy_authorized_pax,
    
    CASE
        WHEN gfa_economy IS NULL AND economy_broken_seats IS NULL THEN NULL
        WHEN gfa_economy IS NOT NULL AND economy_broken_seats IS NULL THEN gfa_economy
        WHEN economy_broken_seats IS NOT NULL AND gfa_economy IS NULL THEN (gfa_economy - economy_broken_seats - COALESCE(jump_seat_quantity, 0))
        WHEN COALESCE(economy_broken_message_timestamp, '1900-01-01 00:00:00') < sabre_transaction_timestamp THEN gfa_economy
        ELSE (gfa_economy - economy_broken_seats)
    END AS economy_authorized_pax

FROM combined_data