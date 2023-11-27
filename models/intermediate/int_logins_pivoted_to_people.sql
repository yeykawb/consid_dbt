--int_logins_pivoted_to_people.sql

with logins as (
    select * from {{ ref('stg_login_service__logins') }}
),

pivot_and_aggregate_logins_to_people_grain as (

    select
        people_id,
        count(login_id) as login_amount
    from logins
    group by 1
)

select * from pivot_and_aggregate_logins_to_people_grain