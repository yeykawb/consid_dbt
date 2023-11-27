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
        concat(firstname, ' ', lastname) as full_name,
        created_at,
        updated_at
    from source
)

select * from renamed