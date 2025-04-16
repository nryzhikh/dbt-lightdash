{% macro radio(value) %}
    {% set pattern = 'радио' %}
    SUM(CASE
        WHEN media ILIKE '%{{ pattern }}%'
        OR formatplacement ILIKE '%{{ pattern }}%'
        OR channel ILIKE '%{{ pattern }}%'
        THEN CAST({{ value }} AS NUMERIC)
        ELSE 0 
    END)
{% endmacro %} 