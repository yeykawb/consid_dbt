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
        deleted_at
    from source
)

select * from deleted_customers
