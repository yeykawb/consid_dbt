--base_login_service__people.sql

{{
  config(
    materialized = 'view'
    )
}}


with source as (

    select * from {{ source('login_service', 'raw_people') }}
),

renamed as (

    select
        id as people_id,
        firstname,
        lastname,
        created_at::timestamp,
        updated_at::timestamp
    from source
)

select * from renamed