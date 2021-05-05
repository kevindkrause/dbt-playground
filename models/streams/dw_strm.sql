{{ config(
	  schema = 'kkrause',
    alias = 'dw',
    materialized = 'incremental',
    unique_key = 'id'
    ) }}

with new_data as(
  select id from wrk_v
)       

select id from new_data