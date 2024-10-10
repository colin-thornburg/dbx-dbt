{{ config(materialized='table') }}

WITH numbered_flights AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY Event_Time_UTC) AS flight_row_number
  FROM {{ ref('flight_data') }}
)

SELECT
  *,
  CONCAT('AA', LPAD(CAST((flight_row_number + 100) AS STRING), 3, '0')) AS flight_number
FROM numbered_flights