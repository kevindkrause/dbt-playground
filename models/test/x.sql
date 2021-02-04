{{ config(
        materialized='incremental',
        unique_key='c_custkey',
        cluster_by='order_dt',
        cluster_key_min_val = 'min_dt',
        cluster_key_max_val = 'max_dt'
         ) }}

with src as (
    select C_CUSTKEY, 'Kevin' as C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, ORDER_DT
    from playground.kkrause.x
    where order_dt ='2021-02-04'
)

select src.*,
  ( select min(order_dt) as min_dt from src ) as min_dt,
  ( select max(order_dt) as max_dt from src ) as max_dt
from src
