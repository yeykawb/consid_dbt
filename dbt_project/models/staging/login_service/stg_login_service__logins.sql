--stg_login_service__logins.sql

with source as (

    select * from {{ source('login_service', 'raw_logins') }}

),

renamed as (
    select
        id::text as login_id,
        logintimestamp::date as date_key,
        userid as people_id
    from source
)

select * from renamed