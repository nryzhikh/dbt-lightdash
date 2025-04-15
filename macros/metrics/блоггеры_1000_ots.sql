
{% macro blogger_ots() %}
    ROUND((
      SUM(CASE
        WHEN channel ILIKE '%блогер%'
        THEN ({{ ots_all() }}) / 1000.0
        ELSE 0
      END)
    )::NUMERIC, 1)
{% endmacro %}
