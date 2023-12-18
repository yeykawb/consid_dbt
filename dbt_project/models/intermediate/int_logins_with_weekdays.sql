--int_logins_pivoted_to_people.sql

with add_dow as (
    select
        login_id,
        {{ dbt_date.day_of_week("date_key") }} as dow_number
    from {{ ref('stg_login_service__logins') }}
),

rename_dow as (
    select 
        login_id,
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
    from add_dow
)

select * from rename_dow