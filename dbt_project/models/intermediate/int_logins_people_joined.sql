with logins as (
    select * from {{ ref("stg_login_service__logins") }}
),

people as (
    select * from {{ ref("stg_login_service__people") }}
),

rename as (
    select
        logins.login_id,
        logins.date_key,
        people.people_id,
        people.full_name
    from logins
    left join people on logins.people_id = people.people_id
)

select * from rename