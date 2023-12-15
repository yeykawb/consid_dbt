--dates.sql

{{
    config(
        materialized = "table"
    )
}}

{{ dbt_date.get_date_dimension("2023-01-01", "2024-01-01") }}