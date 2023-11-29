{% snapshot people_history %}

{{
   config(
       target_database='postgres',
       target_schema='temporal_data',
       unique_key='people_id',
       strategy='timestamp', 
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

select * from {{ ref('stg_login_service__people') }}

{% endsnapshot %}