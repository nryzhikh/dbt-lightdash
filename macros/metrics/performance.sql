{% macro performance(value, additional_patterns=none) %}
    {% set base_patterns = ['performance', 'перформ'] %}
    SUM(CASE
        WHEN 
            {% for base_pattern in base_patterns %}
                {% if additional_patterns is not none %}
                    {% for additional_pattern in additional_patterns %}
                        (
                            media ILIKE '%{{ base_pattern }}%' AND media ILIKE '%{{ additional_pattern }}%'
                            OR formatplacement ILIKE '%{{ base_pattern }}%' AND formatplacement ILIKE '%{{ additional_pattern }}%'
                            OR channel ILIKE '%{{ base_pattern }}%' AND channel ILIKE '%{{ additional_pattern }}%'
                        )
                        {% if not loop.last %}OR{% endif %}
                    {% endfor %}
                {% else %}
                    media ILIKE '%{{ base_pattern }}%'
                    OR formatplacement ILIKE '%{{ base_pattern }}%'
                    OR channel ILIKE '%{{ base_pattern }}%'
                {% endif %}
                {% if not loop.last %}OR{% endif %}
            {% endfor %}
        THEN CAST({{ value }} AS NUMERIC)
        ELSE 0 
    END)
{% endmacro %} 


