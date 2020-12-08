{{
  config(
    materialized='incremental',
    unique_key='id'
  )
}}

with final as
(
    select id, first_name, last_name, email, gender, ip_address, load_dttm, action, isupdate, row_id
    from ode.raw.test_json_strm_v
)

select * from final
