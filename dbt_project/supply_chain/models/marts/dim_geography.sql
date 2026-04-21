/*
    Geography dimension: one row per unique shipping destination.
    
    Fix: the raw data has slightly different lat/long values for the same
    city across different orders (GPS precision varies). We GROUP BY the
    location identifiers and take the average lat/long instead of
    SELECT DISTINCT, which was creating duplicates.
*/

with locations as (
    select
        order_city,
        order_state,
        order_country,
        order_region,
        market,
        round(avg(latitude), 4)   as latitude,
        round(avg(longitude), 4)  as longitude
    from {{ ref('stg_supply_chain') }}
    group by
        order_city,
        order_state,
        order_country,
        order_region,
        market
)

select
    md5(order_city || '-' || order_state || '-' || order_country) as geography_id,
    order_city,
    order_state,
    order_country,
    order_region,
    market,
    latitude,
    longitude
from locations