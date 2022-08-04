{{ config(
        materialized = 'incremental', 
        insert_only = 'true' ) 
}}

with src as (
    select 1 as id
    union all
    select 2
    union all select 3 union all select 4
)

select id as order_id, null as store_id, 'C' as order_status_cd, 0 as total_amt, current_date as order_dt, 
    '1' as priority_cd, '123' as store_emp_id, null as order_cd, '' as notes, null as closed_dt
from src


