--logins.sql

{{
  config(
    materialized = 'incremental',
    schema = 'gold',
    unique_key = 'login_id',
    on_schema_change = 'append_new_columns',
    incremental_strategy = 'merge'
    )
}}

with logins as (
    select * from {{ ref("stg_login_service__logins") }}
),

joined as (
    select * from {{ ref("int_logins_people_joined") }}
),

dow as (
    select * from {{ ref("int_logins_with_weekdays") }}
)

select 
    l.login_id,
    l.date_key,
    j.people_id,
    j.full_name,
    d.day_of_week 
from logins l
left join joined j on l.login_id = j.login_id
left join dow d on l.login_id = d.login_id

{% if is_incremental() %}

    where date_key > (select max(date_key) from {{ this }})

{% endif %}