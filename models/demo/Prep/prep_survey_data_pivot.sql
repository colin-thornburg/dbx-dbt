-- models/transformed/survey_data_wide.sql

{{ config(materialized='table') }}

WITH survey_data AS (
    SELECT * FROM {{ source('survey_data', 'structured_survey_data') }}
),

-- Pivot the data to wide format
pivoted_data AS (
    SELECT
        SurveyResponseIdentifier,
        MAX(CASE WHEN AttributeName = 'RESPONSE_ID' THEN AttributeValue END) AS RESPONSE_ID,
        MAX(CASE WHEN AttributeName = 'INTERVIEW_START_DTTM' THEN AttributeValue END) AS INTERVIEW_START_DTTM,
        MAX(CASE WHEN AttributeName = 'INTERVIEW_END_DTTM' THEN AttributeValue END) AS INTERVIEW_END_DTTM,
        MAX(CASE WHEN AttributeName = 'SURVEY_RESPNS_AUTO_CMPLT_IND' THEN AttributeValue END) AS SURVEY_RESPNS_AUTO_CMPLT_IND,
        MAX(CASE WHEN AttributeName = 'LANGUAGE_PREFERENCE_CD' THEN AttributeValue END) AS LANGUAGE_PREFERENCE_CD,
        MAX(CASE WHEN AttributeName = 'USER_DEVICE_DESC' THEN AttributeValue END) AS USER_DEVICE_DESC,
        MAX(CASE WHEN AttributeName = 'USER_BROWSER_DESC' THEN AttributeValue END) AS USER_BROWSER_DESC,
        MAX(CASE WHEN AttributeName = 'FLIGHT_DT' THEN AttributeValue END) AS FLIGHT_DT,
        MAX(CASE WHEN AttributeName = 'SEG_DEP_TM' THEN AttributeValue END) AS SEG_DEP_TM,
        MAX(CASE WHEN AttributeName = 'OPERAT_AIRLN_NM' THEN AttributeValue END) AS OPERAT_AIRLN_NM,
        MAX(CASE WHEN AttributeName = 'MKT_FLIGHT_NBR' THEN AttributeValue END) AS MKT_FLIGHT_NBR,
        MAX(CASE WHEN AttributeName = 'OPERAT_FLIGHT_NBR' THEN AttributeValue END) AS OPERAT_FLIGHT_NBR,
        MAX(CASE WHEN AttributeName = 'OD_ORIGIN_AIRPRT_IATA_CD' THEN AttributeValue END) AS OD_ORIGIN_AIRPRT_IATA_CD,
        MAX(CASE WHEN AttributeName = 'OD_DESTNTN_AIRPRT_IATA_CD' THEN AttributeValue END) AS OD_DESTNTN_AIRPRT_IATA_CD,
        MAX(CASE WHEN AttributeName = 'LIKELIHOOD_RECOMMEND_SCR' THEN AttributeValue END) AS LIKELIHOOD_RECOMMEND_SCR,
        MAX(CASE WHEN AttributeName = 'TRIP_PURPOSE_CD' THEN AttributeValue END) AS TRIP_PURPOSE_CD,
        MAX(CASE WHEN AttributeName = 'TRAVEL_OPRTNTY_WORK_REMOTE_DESTNTN_IND' THEN AttributeValue END) AS TRAVEL_OPRTNTY_WORK_REMOTE_DESTNTN_IND
    FROM survey_data
    GROUP BY SurveyResponseIdentifier
),

-- Create a composite key for joining with flight data
final AS (
    SELECT 
        *,
        CONCAT(
            COALESCE(FLIGHT_DT, ''), '_',
            COALESCE(OPERAT_FLIGHT_NBR, ''), '_',
            COALESCE(OD_ORIGIN_AIRPRT_IATA_CD, ''), '_',
            COALESCE(OD_DESTNTN_AIRPRT_IATA_CD, '')
        ) AS composite_key
    FROM pivoted_data
)

SELECT * FROM final