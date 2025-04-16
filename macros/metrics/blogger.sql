{% macro blogger(value) %}
    {% set patterns = ['блогер', 'blogger'] %}
    SUM(CASE
        WHEN 
            {% for pattern in patterns %}
                media_type ILIKE '%{{ pattern }}%'
                OR media ILIKE '%{{ pattern }}%'
                OR formatplacement ILIKE '%{{ pattern }}%'
                OR channel ILIKE '%{{ pattern }}%'
                {% if not loop.last %}OR{% endif %}
            {% endfor %}
        THEN CAST({{ value }} AS NUMERIC)
        ELSE 0 
    END)
{% endmacro %} 


