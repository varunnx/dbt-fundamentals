select 
    id as payments_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    amount as payment_amount,
    created as payment_created
 FROM raw.stripe.payment