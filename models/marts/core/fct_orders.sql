with orders as
(
    select 
        order_id,
        customer_id 
    FROM {{ ref('stg_orders') }}
),

payments as 
(
    select 
         order_id,
         payment_amount
    from {{ ref('stg_payments') }}
)

select o.order_id as order_id,
    o.customer_id as customer_id,
    p.payment_amount as amount
from orders o inner join payments p on o.order_id = p.order_id
