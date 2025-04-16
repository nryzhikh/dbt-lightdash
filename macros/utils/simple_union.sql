{% macro simple_union(tables, date_mapping={}) %}
    {# Get all columns from all tables #}
    {% set all_columns = {} %}
    {% set all_tables = [] %}
    
    {% for table in tables %}
        {% set table_parts = table.split('.') %}
        {% set source_name = table_parts[0] %}
        {% set table_name = table_parts[1] %}
        {% set table_columns = adapter.get_columns_in_relation(source(source_name, table_name)) %}
        {% set table_col_names = table_columns | map(attribute='name') | list %}
        {% do all_columns.update({table: table_col_names}) %}
        {% do all_tables.append(table) %}
    {% endfor %}
    
    {# Get all unique column names across all tables #}
    {% set all_unique_columns = [] %}
    {% for table in all_tables %}
        {% for col in all_columns[table] %}
            {% if col not in all_unique_columns and col not in date_mapping.values() %}
                {% do all_unique_columns.append(col) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    
    {# Generate the UNION ALL query for each table #}
    {% for table in all_tables %}
        {% if not loop.first %}
            UNION ALL
        {% endif %}
        
        {% set table_parts = table.split('.') %}
        {% set source_name = table_parts[0] %}
        {% set table_name = table_parts[1] %}
        
        SELECT
            {# Handle all columns, including missing ones #}
            {% for col in all_unique_columns %}
                {% if col in all_columns[table] %}
                    {% if col in date_mapping.keys() %}
                        {# If this is a source column for date mapping, use the target column if it exists #}
                        {% set target_col = date_mapping[col] %}
                        {% if target_col in all_columns[table] %}
                            CAST("{{ target_col }}" AS TEXT) as "{{ col }}",
                        {% else %}
                            CAST("{{ col }}" AS TEXT) as "{{ col }}",
                        {% endif %}
                    {% else %}
                        CAST("{{ col }}" AS TEXT) as "{{ col }}",
                    {% endif %}
                {% else %}
                    NULL::TEXT as "{{ col }}",
                {% endif %}
            {% endfor %}
            
            {# Date mapping columns #}
            {% for source_col, target_col in date_mapping.items() %}
                {% if source_col in all_columns[table] %}
                    CASE 
                        WHEN CAST("{{ source_col }}" AS TEXT) ~ '^\d+$' THEN 
                            (DATE '1899-12-30' + CAST(CAST("{{ source_col }}" AS TEXT) AS INTEGER))::TEXT
                        ELSE 
                            CAST("{{ source_col }}" AS TEXT)
                    END as "{{ target_col }}",
                {% elif target_col in all_columns[table] %}
                    CAST("{{ target_col }}" AS TEXT) as "{{ target_col }}",
                {% else %}
                    NULL::TEXT as "{{ target_col }}",
                {% endif %}
            {% endfor %}
            
            '{{ table }}' as source_table
        FROM {{ source(source_name, table_name) }}
    {% endfor %}
{% endmacro %} 
