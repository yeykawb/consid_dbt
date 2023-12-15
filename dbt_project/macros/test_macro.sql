{% macro test_macro() -%}

    {%- set target_column_name = '_dbt_loaded_at' %}
    
    {%- set columns = adapter.get_columns_in_relation(this) %}
    
    {%- if target_column_name not in columns|map(attribute='column_name') %}
        {%- set add_column_query = "ALTER TABLE " ~ this.identifier ~ " ADD COLUMN " ~ target_column_name ~ " TIMESTAMP DEFAULT CURRENT_TIMESTAMP" %}
        {{ run_query(add_column_query) }}
    {%- endif %}

{% endmacro %}
