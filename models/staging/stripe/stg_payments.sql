with payments as
(
    select id as payment_id, orderid as order_id, amount/100 as amount, status from stripe.payment
)

select * from payments