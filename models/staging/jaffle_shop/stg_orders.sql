with orders as (

    select
        {{ dbt_utils.surrogate_key(['user_id','order_date']) }} as ak,
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from {{ source('jaffle_shop','orders') }}

)

select * from orders