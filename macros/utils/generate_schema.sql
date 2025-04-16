{% macro generate_schema(table_name) %}
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = '{{ table_name }}'
    ORDER BY ordinal_position;
{% endmacro %} 