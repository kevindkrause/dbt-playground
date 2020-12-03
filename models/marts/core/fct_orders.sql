with orders as (
    select * from {{ ref('stg_orders') }}

),

payments as (
    select * from {{ ref('stg_payments') }}
),

final as (

    select o.order_id, o.customer_id, p.amount
    from orders o
    inner join payments p
        on o.order_id = p.order_id
)

select * from final
