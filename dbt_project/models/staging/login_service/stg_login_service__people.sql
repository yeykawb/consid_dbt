--stg_login_service__people.sql

{{
  config(
    materialized = 'table'
    )
}}


with people as (

    select * from {{ ref('base_login_service__people') }}
),

deleted_people as (

    select * from {{ ref('base_login_service__deleted_people') }}
),

join_and_mark_deleted_people as (

    select
        people.people_id,
        concat(people.firstname, ' ', people.lastname) as full_name,
        people.created_at,
        people.updated_at,
        deleted_people.is_deleted
        
    from people
    left join deleted_people on people.people_id = deleted_people.people_id
)

select * from join_and_mark_deleted_people