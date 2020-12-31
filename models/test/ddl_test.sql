{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id'
  )
}}

with big_view as (

    select 4 as id, current_date as dt, 'XX' as usr
    union all 
    select 2 as id, current_date as dt, 'NC' as usr
)

select * from big_view