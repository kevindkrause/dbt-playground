{{
  config(
    materialized='view',
  )
}}

with final as
(
    select current_date() as dt
)

select * from final