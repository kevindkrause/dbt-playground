{{ config(
        materialized = 'incremental',
        unique_key = 'c_custkey',
        cluster_by = 'order_dt',
        cluster_key_min_val = 'min_dt',
        cluster_key_max_val = 'max_dt'
         ) }}

with src as (
    select c_custkey, 'Kevin' as c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, order_dt
    from playground.kkrause.x
    where order_dt ='2021-02-04'
)

select src.*,
  ( select min(order_dt) as min_dt from src ) as min_dt,
  ( select max(order_dt) as max_dt from src ) as max_dt
from src
