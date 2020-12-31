{{
  config(
    materialized='view',
  )
}}

with big_view as (

    select
       store_id,
       order_dt as order_date,
       count(*) as order_cnt
    from demo_db.kkrause.order_by_dt_1t
    where store_id = 83202646
    group by store_id, order_dt
    order by order_dt
)

select * from big_view