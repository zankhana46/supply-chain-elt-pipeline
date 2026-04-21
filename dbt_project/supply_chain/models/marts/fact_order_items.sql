/*
    Fact table: one row per order line item.
    
    This is the center of the star schema. It holds:
    1. Foreign keys pointing to each dimension table
    2. Measures (numbers you aggregate: SUM, AVG, COUNT)
    
    An analyst can JOIN this to any dimension to answer questions like:
    "What's the average delivery delay by shipping mode in Europe?"
    fact_order_items → dim_shipping (for mode) → dim_geography (for region)
*/

select
    -- Natural keys from source
    order_id,
    order_item_id,
    
    -- Foreign keys to dimensions
    customer_id,
    product_id,
    md5(order_city || '-' || order_state || '-' || order_country) as geography_id,
    md5(shipping_mode || '-' || order_status) as shipping_id,
    cast(order_date as date)            as order_date_key,
    cast(shipping_date as date)         as shipping_date_key,
    
    -- Shipping performance measures
    shipping_days_real,
    shipping_days_scheduled,
    shipping_days_real - shipping_days_scheduled as shipping_delay_days,
    late_delivery_risk,
    delivery_status,
    
    -- Financial measures
    order_type,
    quantity,
    sales,
    discount,
    discount_rate,
    item_price,
    item_total,
    benefit_per_order,
    profit_ratio,
    profit_per_order,
    sales_per_customer

from {{ ref('stg_supply_chain') }}