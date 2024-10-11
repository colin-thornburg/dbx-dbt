{{ config(materialized='table') }}

{%- set attribute_names = dbt_utils.get_column_values(
    table=ref('prep_survey'),
    column='AttributeName'
) -%}

WITH survey_data AS (
  SELECT
    SurveyResponseIdentifier,
    SurveyIdentifier,
    DistributionIdentifier,
    SurveyResponseBeginTimestamp,
    SurveyResponseEndTimestamp,
    SurveyResponseUpdateTimestamp,
    SourceExtractBeginTimestamp,
    flight_number,
    AttributeName,
    AttributeValue
  FROM {{ ref('prep_survey') }}
)

SELECT 
  SurveyResponseIdentifier,
  SurveyIdentifier,
  DistributionIdentifier,
  SurveyResponseBeginTimestamp,
  SurveyResponseEndTimestamp,
  SurveyResponseUpdateTimestamp,
  SourceExtractBeginTimestamp,
  flight_number,
  {% for attr in attribute_names %}
  MAX(CASE WHEN AttributeName = '{{ attr }}' THEN AttributeValue END) AS {{ attr }}{{ ", " if not loop.last else "" }}
  {% endfor %}
FROM survey_data
GROUP BY 
  SurveyResponseIdentifier,
  SurveyIdentifier,
  DistributionIdentifier,
  SurveyResponseBeginTimestamp,
  SurveyResponseEndTimestamp,
  SurveyResponseUpdateTimestamp,
  SourceExtractBeginTimestamp,
  flight_number