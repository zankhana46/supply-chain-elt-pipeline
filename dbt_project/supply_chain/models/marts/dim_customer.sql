/*
    Customer dimension: one row per unique customer.
    
    The raw data repeats customer info on every order row.
    A customer who ordered 50 times appears 50 times in raw data.
    Here we deduplicate to get one clean row per customer.
    
    Why this matters: without this, if a customer's segment changes,
    you'd have conflicting info across rows. The dimension table
    is the single source of truth for "who is this customer?"
*/

with customers as (
    select
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_segment,
        customer_city,
        customer_state,
        customer_country,
        customer_zipcode,
        -- ROW_NUMBER picks the most recent record per customer
        -- In case a customer's info changed across orders
        row_number() over (
            partition by customer_id 
            order by order_date desc
        ) as row_num
    from {{ ref('stg_supply_chain') }}
)

select
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_segment,
    customer_city,
    customer_state,
    customer_country,
    customer_zipcode
from customers
where row_num = 1