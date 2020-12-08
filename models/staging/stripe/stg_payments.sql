with payments as
(
    select id as payment_id, orderid as order_id, {{ cents_to_dollars( 'amount', 4 ) }} as amount, status, paymentmethod as payment_method from stripe.payment
)

select * from payments