WITH source AS (
    SELECT * FROM {{ ref('analyst_flight_delays') }}
)

SELECT
    FLIGHT_KEY_CD AS flight_key,
    CAST(SCHEDULED_DEPARTURE_TIME AS TIMESTAMP) AS scheduled_departure_time,
    CAST(ACTUAL_DEPARTURE_TIME AS TIMESTAMP) AS actual_departure_time,
    CAST(DELAY_MINUTES AS INT) AS delay_minutes
FROM source