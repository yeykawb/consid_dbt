{% macro generate_loaded_at_column(tables) -%}
    {%- for table in tables -%}
        {%- set node = ref(table) -%}
        {% set query %}
            DO $$ 
            BEGIN
                -- Check if the column already exists
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_schema = '{{ node.schema }}' 
                    AND table_name = '{{ node.identifier }}' 
                    AND column_name = '_dbt_loaded_at'
                    ) 
                THEN
                    -- If not exists, then add the column
                    EXECUTE 'ALTER TABLE ' || quote_ident('{{ node.schema }}') || '.' || quote_ident('{{ node.identifier }}') || ' ADD COLUMN _dbt_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP';
                END IF;
            END $$;
        {% endset %}
        {{ run_query(query) }}
    {%- endfor -%}
    
    {# /* run below command to insert column for each seed */ #}
    {# /* dbt run-operation generate_loaded_at_column --args '{tables: [raw_logins, raw_people, raw_people_deleted]}' */ #}
{%- endmacro %}
