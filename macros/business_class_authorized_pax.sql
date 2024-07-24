{% macro business_class_authorized_pax() %}
    CASE
        WHEN gfa_business_class IS NULL AND business_class_broken_seats IS NULL THEN NULL
        WHEN gfa_business_class IS NOT NULL AND business_class_broken_seats IS NULL THEN gfa_business_class
        WHEN business_class_broken_seats IS NOT NULL AND gfa_business_class IS NULL THEN (gfa_business_class - business_class_broken_seats)
        WHEN COALESCE(business_class_broken_message_timestamp, '1900-01-01 00:00:00') < sabre_transaction_timestamp THEN gfa_business_class
        ELSE (gfa_business_class - business_class_broken_seats)
    END
{% endmacro %}