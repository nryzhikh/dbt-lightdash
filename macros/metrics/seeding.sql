{% macro seeding(value) %}
    {% set pattern = 'посев' %}
    SUM(CASE
        WHEN media ILIKE '%{{ pattern }}%'
        OR formatplacement ILIKE '%{{ pattern }}%'
        OR channel ILIKE '%{{ pattern }}%'
        OR _path_level_1 ILIKE '%{{ pattern }}%'
        THEN CAST({{ value }} AS NUMERIC)
        ELSE 0 
    END)
{% endmacro %} 


