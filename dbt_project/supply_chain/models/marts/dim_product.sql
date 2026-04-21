/*
    Product dimension: one row per unique product.
    Includes category and department as these are attributes OF the product.
*/

with products as (
    select
        product_id,
        product_name,
        category_id,
        category_name,
        department_id,
        department_name,
        product_price,
        row_number() over (
            partition by product_id 
            order by order_date desc
        ) as row_num
    from {{ ref('stg_supply_chain') }}
)

select
    product_id,
    product_name,
    category_id,
    category_name,
    department_id,
    department_name,
    product_price
from products
where row_num = 1