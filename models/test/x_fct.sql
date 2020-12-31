{{
  config(
    schema='kkrause',
    materialized='incremental',
    unique_key='order_id',
    cluster_by='order_dt'
  )
}}

with src as (
    select 1 as order_id, 99 as store_id, 'XX' as order_status_cd, 1 as total_amt,'2019-12-15' as order_dt,'X' as priority_cd,1 as store_emp_id, 123 as order_cd, 'blah' as notes
    union all 
    select 2 as order_id, 99 as store_id, 'XX' as order_status_cd, 1 as total_amt,'2020-12-31' as order_dt,'X' as priority_cd,1 as store_emp_id, 123 as order_cd, 'blah' as notes
)
select src.*, 
  (select min(order_dt) as min_dt from src) as min_dt,
  (select max(order_dt) as max_dt from src) as max_dt
from src
