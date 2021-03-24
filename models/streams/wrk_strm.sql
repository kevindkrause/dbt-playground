{{ config(
    schema = 'kkrause',
    alias = 'wrk',
    materialized = 'incremental',
    unique_key = 'id'
    ) }}

with new_data as(
  select id from {{ref('stg')}}
)       

select id from new_data