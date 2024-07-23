WITH source AS (
    SELECT * FROM {{ ref('jump_seats') }}
)

SELECT
    OPERAT_AIRLN_IATA_CD AS airline_code,
    OPERAT_FLIGHT_NBR AS flight_number,
    SCHD_LEG_DEP_LCL_DT AS departure_date,
    SCHD_LEG_DEP_AIRPRT_IATA_CD AS departure_airport,
    FLIGHT_KEY_CD AS flight_key,
    JUMP_SEAT_QTY AS jump_seat_quantity
FROM source