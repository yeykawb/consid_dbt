--int_logins_pivoted_to_people.sql

with logins as (
    select * from {{ ref('stg_login_service__logins') }}
),

add_dow as (
    select
        logins.login_id,
        {{ dbt_date.day_of_week("date_key") }} as dow_number
    from logins
),

rename_dow as (
    select 
        logins.*,
        case add_dow.dow_number
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
            when 7 then 'Sunday'
            else 'Unknown'
        end as day_of_week 
from logins
left join add_dow
on logins.login_id = add_dow.login_id
)

select * from rename_dow




        