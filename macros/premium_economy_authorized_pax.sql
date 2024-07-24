{% macro premium_economy_authorized_pax() %}
    CASE
        WHEN gfa_premium_economy IS NULL AND premium_economy_broken_seats IS NULL THEN NULL
        WHEN gfa_premium_economy IS NOT NULL AND premium_economy_broken_seats IS NULL THEN gfa_premium_economy
        WHEN premium_economy_broken_seats IS NOT NULL AND gfa_premium_economy IS NULL THEN (gfa_premium_economy - premium_economy_broken_seats)
        WHEN COALESCE(premium_economy_broken_message_timestamp, '1900-01-01 00:00:00') < sabre_transaction_timestamp THEN gfa_premium_economy
        ELSE (gfa_premium_economy - premium_economy_broken_seats)
    END
{% endmacro %}