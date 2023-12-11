--base_login_service__deleted_people.sql

{{
  config(
    materialized = 'view'
    )
}}

with source as (

    select * from {{ source('login_service', 'raw_people') }}
),

deleted_customers as (

    select
        id as people_id,
        deleted_at::timestamp
    from source
)

select * from deleted_customers