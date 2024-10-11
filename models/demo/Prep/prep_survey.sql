{{ config(materialized='table') }}

WITH distinct_surveys AS (
  SELECT DISTINCT
    SurveyResponseIdentifier
  FROM {{ source('survey_data','cast_survey_data') }}
),

numbered_surveys AS (
  SELECT 
    SurveyResponseIdentifier,
    ROW_NUMBER() OVER (ORDER BY SurveyResponseIdentifier) AS survey_row_number
  FROM distinct_surveys
),

flight_numbers AS (
  SELECT 
    SurveyResponseIdentifier,
    CONCAT('AA', LPAD(CAST((survey_row_number + 100) AS STRING), 3, '0')) AS flight_number
  FROM numbered_surveys
)

SELECT
  s.*,
  f.flight_number
FROM {{ source('survey_data','cast_survey_data') }} s
JOIN flight_numbers f ON s.SurveyResponseIdentifier = f.SurveyResponseIdentifier