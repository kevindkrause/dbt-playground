{{ config(
	    schema ='kkrause',
      materialized = 'incremental',
      unique_key = 'id'
       ) }}

with new_data as(
  select id from raw_v
)       

select id from new_data