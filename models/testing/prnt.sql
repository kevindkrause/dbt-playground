{{
  config(
    materialized='incremental',
    unique_key='prnt_id'
  )
}}

with final as
(
    select 1 as prnt_id
)

select * from final
