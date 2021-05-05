{{
  config(
    materialized='incremental',
    unique_key='chld_id'
  )
}}

with final as
(
    select -1 as chld_id, 1 as prnt_id
)

select * from final
