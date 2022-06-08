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
         SUM(payment_amount) AS payment_amount
    from {{ ref('stg_payments') }}
    where status = "success"
    group by 1
)

select o.order_id as order_id,
    o.customer_id as customer_id,
    p.payment_amount as amount
from orders o inner join payments p on o.order_id = p.order_id
