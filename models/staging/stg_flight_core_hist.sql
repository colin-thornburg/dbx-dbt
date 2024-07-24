WITH source AS (
    SELECT * FROM {{ ref('flight_core_hist') }}
),

renamed AS (
    SELECT
        OPERAT_AIRLN_IATA_CD AS airline_code,
        OPERAT_FLIGHT_NBR AS flight_number,
        SCHD_LEG_DEP_LCL_DT AS departure_date,
        SCHD_LEG_DEP_AIRPRT_IATA_CD AS departure_airport,
        FLIGHT_KEY_CD AS flight_key,
        TRANS_TXT AS transaction_text,
        SABRE_TRANS_UTC_TMS AS sabre_transaction_timestamp,
        SCHD_AIRCFT_TYPE_CD AS aircraft_type,
        SCHD_AIRCFT_CONFIG_CD AS aircraft_config,
        CASE 
            WHEN TRANS_TXT LIKE 'GFA@%' THEN 
                CAST(regexp_extract(TRANS_TXT, 'F(\\d+)', 1) AS INT)
            ELSE NULL 
        END AS gfa_first_class,
        CASE 
            WHEN TRANS_TXT LIKE 'GFA@%' THEN 
                CAST(regexp_extract(TRANS_TXT, 'C(\\d+)', 1) AS INT)
            ELSE NULL 
        END AS gfa_business_class,
        CASE 
            WHEN TRANS_TXT LIKE 'GFA@%' THEN 
                CAST(regexp_extract(TRANS_TXT, 'W(\\d+)', 1) AS INT)
            ELSE NULL 
        END AS gfa_premium_economy,
        CASE 
            WHEN TRANS_TXT LIKE 'GFA@%' THEN 
                CAST(regexp_extract(TRANS_TXT, 'Y(\\d+)', 1) AS INT)
            ELSE NULL 
        END AS gfa_economy
    FROM source
),

final AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY airline_code, flight_number, departure_date, departure_airport 
            ORDER BY sabre_transaction_timestamp DESC
        ) AS row_num
    FROM renamed
)

SELECT * EXCEPT(row_num)
FROM final
WHERE row_num = 1