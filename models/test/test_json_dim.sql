{{
  config(
    materialized='incremental',
    unique_key='test_json_id'
  )
}}

with final as
(
    select id as test_json_id, first_name as first_nm, last_name as last_nm, full_name as full_nm, email as email_addr, gender as gender_cd, ip_address as ip_addr, load_dttm, row_id as src_row_id
    from {{ ref('test_json_tmp') }}
)

select * from final