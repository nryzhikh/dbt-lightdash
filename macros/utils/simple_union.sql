{% macro simple_union(table1, table2, date_mapping={}) %}
    {% set table1_columns = adapter.get_columns_in_relation(source(table1.split('.')[0], table1.split('.')[1])) %}
    {% set table2_columns = adapter.get_columns_in_relation(source(table2.split('.')[0], table2.split('.')[1])) %}
    
    {% set table1_col_names = table1_columns | map(attribute='name') | list %}
    {% set table2_col_names = table2_columns | map(attribute='name') | list %}
    
    {% set common_columns = [] %}
    {% set table1_only_columns = [] %}
    {% set table2_only_columns = [] %}
    
    {# Handle date mapping columns separately #}
    {% set mapped_columns = date_mapping.keys() | list %}
    
    {% for col in table1_col_names %}
        {% if col in table2_col_names and col not in mapped_columns %}
            {% do common_columns.append(col) %}
        {% elif col not in mapped_columns %}
            {% do table1_only_columns.append(col) %}
        {% endif %}
    {% endfor %}
    
    {% for col in table2_col_names %}
        {% if col not in table1_col_names and col not in mapped_columns %}
            {% do table2_only_columns.append(col) %}
        {% endif %}
    {% endfor %}
    
    SELECT
        {% for col in common_columns %}
            CAST("{{ col }}" AS TEXT) as "{{ col }}",
        {% endfor %}
        {% for col in table1_only_columns %}
            CAST("{{ col }}" AS TEXT) as "{{ col }}",
        {% endfor %}
        {% for col in table2_only_columns %}
            NULL::TEXT as "{{ col }}",
        {% endfor %}
        {% for source_col, target_col in date_mapping.items() %}
            CASE 
                WHEN "{{ source_col }}" ~ '^\d+$' THEN 
                    (DATE '1899-12-30' + CAST("{{ source_col }}" AS INTEGER))::TEXT
                ELSE 
                    CAST("{{ source_col }}" AS TEXT)
            END as "{{ source_col }}",
        {% endfor %}
        '{{ table1 }}' as source_table
    FROM {{ source(table1.split('.')[0], table1.split('.')[1]) }}
    
    UNION ALL
    
    SELECT
        {% for col in common_columns %}
            CAST("{{ col }}" AS TEXT) as "{{ col }}",
        {% endfor %}
        {% for col in table1_only_columns %}
            NULL::TEXT as "{{ col }}",
        {% endfor %}
        {% for col in table2_only_columns %}
            CAST("{{ col }}" AS TEXT) as "{{ col }}",
        {% endfor %}
        {% for source_col, target_col in date_mapping.items() %}
            CASE 
                WHEN "{{ target_col }}" ~ '^\d+$' THEN 
                    (DATE '1899-12-30' + CAST("{{ target_col }}" AS INTEGER))::TEXT
                ELSE 
                    CAST("{{ target_col }}" AS TEXT)
            END as "{{ source_col }}",
        {% endfor %}
        '{{ table2 }}' as source_table
    FROM {{ source(table2.split('.')[0], table2.split('.')[1]) }}
{% endmacro %} 