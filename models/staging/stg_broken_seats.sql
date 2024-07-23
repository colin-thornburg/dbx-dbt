WITH source AS (
    SELECT * FROM {{ ref('broken_seats') }}
)

SELECT
    OPERAT_AIRLN_IATA_CD AS airline_code,
    OPERAT_FLIGHT_NBR AS flight_number,
    SCHD_LEG_DEP_LCL_DT AS departure_date,
    SCHD_LEG_DEP_AIRPRT_IATA_CD AS departure_airport,
    FLIGHT_KEY_CD AS flight_key,
    F_CABIN_CD AS first_class_cabin_code,
    C_CABIN_CD AS business_class_cabin_code,
    W_CABIN_CD AS premium_economy_cabin_code,
    Y_CABIN_CD AS economy_cabin_code,
    F_BRK_MSG_CREATE_TMS AS first_class_broken_message_timestamp,
    C_BRK_MSG_CREATE_TMS AS business_class_broken_message_timestamp,
    W_BRK_MSG_CREATE_TMS AS premium_economy_broken_message_timestamp,
    Y_BRK_MSG_CREATE_TMS AS economy_broken_message_timestamp,
    F_BRKN_SEATS AS first_class_broken_seats,
    C_BRKN_SEATS AS business_class_broken_seats,
    W_BRKN_SEATS AS premium_economy_broken_seats,
    Y_BRKN_SEATS AS economy_broken_seats
FROM source