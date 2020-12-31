{{
  config(
    materialized='table',
    schema='STG'
  )
}}

with final as (
    select 1 as id
)
select * from final