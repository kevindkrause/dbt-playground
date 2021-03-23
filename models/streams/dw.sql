{{ config(
	  schema ='kkrause',
      materialized = 'incremental',
      unique_key = 'id'
       ) }}

with new_data as(
  select id from {{ref('wrk')}}
)       

select id from new_data