--base_login_service__deleted_people.sql

{{
  config(
    materialized = 'view'
    )
}}

with source as (

    select * from {{ source('login_service', 'raw_people_deleted') }}
),

deleted_customers as (

    select
        id as people_id,
        deleted as is_deleted
    from source
)

select * from deleted_customers