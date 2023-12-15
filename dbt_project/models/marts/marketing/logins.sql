--logins.sql

{{
  config(
    materialized = 'incremental',
    schema = 'gold',
    unique_key = 'login_id',
    on_schema_change = 'append_new_columns',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['_dbt_inserted_at']
    )
}}

with joined as (
    select * from {{ ref("int_logins_people_joined") }}
),

final as (
    select
        *,
        date(login_date) as date_key
    from joined
)

select * from final

{% if is_incremental() %}

    where login_date > (select max(login_date) from {{ this }})

{% endif %}