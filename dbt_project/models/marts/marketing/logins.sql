--logins.sql

{{
  config(
    materialized = 'incremental',
    unique_key = 'login_id',
    on_schema_change = 'append_new_columns',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['_dbt_inserted_at']
    )
}}

with logins as (
    select * from {{ ref("stg_login_service__logins") }}
),

people as (
    select * from {{ ref("stg_login_service__people") }}
),

rename as (
    select
        logins.login_id,
        logins.login_timestamp,
        logins.day_of_week,
        logins.people_id,
        people.full_name
    from logins
    left join people on logins.people_id = people.people_id
),

final as (
    select
        *,
        date(login_timestamp) as date_key,
        {{ 
            dbt_utils.generate_surrogate_key(
                dbt_utils.get_filtered_columns_in_relation(
                        from=ref("stg_login_service__logins")
                )
            )
        }} as _dbt_hash,
        current_timestamp as _dbt_inserted_at,
        current_timestamp as _dbt_updated_at
    from rename
)

select * from final

{% if is_incremental() %}

    where _dbt_hash not in (select _dbt_hash from {{ this }})

{% endif %}