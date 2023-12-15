{% macro generate_loaded_at_column() -%}

    {% set query %}
    SELECT
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = concat("'", {{this.name}}, "'")
                AND column_name = '_dbt_loaded_at'
            )
            THEN 1
            ELSE 0
        END AS column_exist
    {% endset %}

    {% set column_exists = dbt_utils.get_single_value(query) %}

    {% if column_exists == 0 %}

        {% set alter_table_query %}
            ALTER TABLE {{ node }} ADD COLUMN _dbt_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        {% endset %}

        {{ run_query(alter_table_query) }}
    
    {% endif %}

{%- endmacro %}
