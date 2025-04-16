{% macro roundv(value, divide_by=1, decimals=0) %}
    ROUND(
        CASE 
            WHEN {{ divide_by }} > 0 THEN CAST({{ value }} AS NUMERIC) / {{ divide_by }}
            ELSE CAST({{ value }} AS NUMERIC)
        END,
        {{ decimals }}
    )
{% endmacro %} 