{% macro tv(value) %}
    {% set patterns = ['тв', 'tv'] %}
    SUM(CASE
        WHEN 
            {% for pattern in patterns %}
                _path_level_1 ILIKE '%{{ pattern }}%'
                OR channel ILIKE '%{{ pattern }}%'
                OR formatplacement ILIKE '%{{ pattern }}%'
                OR channel ILIKE '%{{ pattern }}%'
                {% if not loop.last %}OR{% endif %}
            {% endfor %}
        THEN CAST({{ value }} AS NUMERIC)
        ELSE 0 
    END)
{% endmacro %} 


