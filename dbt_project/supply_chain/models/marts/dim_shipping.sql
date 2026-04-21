/*
    Shipping dimension: one row per unique shipping mode + order status combo.
    Lightweight dimension that describes HOW an order was shipped.
*/

with shipping as (
    select distinct
        shipping_mode,
        order_status
    from {{ ref('stg_supply_chain') }}
)

select
    md5(shipping_mode || '-' || order_status) as shipping_id,
    shipping_mode,
    order_status
from shipping