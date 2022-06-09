with customers as (
select
	*
from
	{{ ref('stg_customers')}}

),

orders as (
select
	*
from
	{{ ref('stg_orders') }}

),

payments as (
select
	*
from
	{{ ref('stg_payments') }}
),

customer_lifetime_value as (
select
	c.customer_id as customer_id,
	SUM(p.payment_amount) as lifetime_value
from
	customers c 
left join orders o on 
    c.customer_id = o.customer_id
left join payments p on
	o.order_id = p.order_id
where
p.payment_status = 'success'
GROUP BY 1
	),

customer_orders as (
select
	customer_id,
	min(order_date) as first_order_date,
	max(order_date) as most_recent_order_date,
	count(order_id) as number_of_orders
from
	orders
group by
	1

),

final as (
select
	customers.customer_id,
	customers.first_name,
	customers.last_name,
	customer_orders.first_order_date,
	customer_orders.most_recent_order_date,
	coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
    coalesce(cltv.lifetime_value, 0)/100.0 as lifetime_value
from
	customers
left join customer_orders
		using (customer_id)
left join customer_lifetime_value cltv
        using (customer_id)
)

select
	*
from
	final