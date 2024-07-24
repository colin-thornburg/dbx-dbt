{% macro first_class_authorized_pax() %}
    CASE
        WHEN gfa_first_class IS NULL AND first_class_broken_seats IS NULL THEN NULL
        WHEN gfa_first_class IS NOT NULL AND first_class_broken_seats IS NULL THEN gfa_first_class
        WHEN first_class_broken_seats IS NOT NULL AND gfa_first_class IS NULL THEN (gfa_first_class - first_class_broken_seats)
        WHEN COALESCE(first_class_broken_message_timestamp, '1900-01-01 00:00:00') < sabre_transaction_timestamp THEN gfa_first_class
        ELSE (gfa_first_class - first_class_broken_seats)
    END
{% endmacro %}