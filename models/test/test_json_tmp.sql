{{
  config(
    materialized='incremental',
    unique_key='id'
  )
}}

with final as
(
    select id, first_name, last_name, last_name || ', ' || first_name as full_name, email, case when gender = 'Female' then 'F' else 'M' end as gender, ip_address, load_dttm, action, isupdate, row_id
    from {{ ref('test_json') }}

)

select * from final
