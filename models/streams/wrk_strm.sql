{{ config(
    schema = 'kkrause',
    alias = 'wrk',
    materialized = 'incremental',
    unique_key = 'id'
    ) }}

with new_data as(
  select id from stg_v
)       

select id from new_data