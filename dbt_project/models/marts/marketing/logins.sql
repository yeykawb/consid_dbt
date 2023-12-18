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

with joined as (
    select * from {{ ref("int_logins_people_joined") }}
),

dow as (
    select * from {{ ref("int_logins_with_weekdays") }}
)

select 
    joined.*,
    dow.day_of_week 
from joined
left join dow
on joined.login_id = dow.login_id

{% if is_incremental() %}

    where date_key > (select max(date_key) from {{ this }})

{% endif %}