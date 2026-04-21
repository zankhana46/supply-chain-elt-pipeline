/*
    Staging model: one-to-one with the source table.
    
    What we do here:
    - Rename ugly columns to clean snake_case
    - Cast data types (dates are strings in raw, we fix that)
    - Drop columns we don't need (passwords, images, etc.)
    - NO business logic here — just cleaning
    
    This runs as a VIEW by default (not a table), which means
    it doesn't duplicate data — it's just a saved SQL query.
*/

with source as (
    select * from {{ source('raw', 'supply_chain') }} --this is Jinja templating. 
),

cleaned as (
    select
        -- Order identifiers
        "Order Id"                          as order_id,
        "Order Item Id"                     as order_item_id,
        "Order Customer Id"                 as order_customer_id,

        -- Order dates (casting string to date)
        -- The raw data stores dates as strings like '1/31/2018 22:56'
        strptime("order date (DateOrders)", '%m/%d/%Y %H:%M')  as order_date,
        strptime("shipping date (DateOrders)", '%m/%d/%Y %H:%M') as shipping_date,

        -- Customer attributes
        "Customer Id"                       as customer_id,
        "Customer Fname"                    as customer_first_name,
        "Customer Lname"                    as customer_last_name,
        "Customer Email"                    as customer_email,
        "Customer Segment"                  as customer_segment,
        "Customer City"                     as customer_city,
        "Customer State"                    as customer_state,
        "Customer Country"                  as customer_country,
        "Customer Zipcode"                  as customer_zipcode,

        -- Product attributes
        "Product Card Id"                   as product_id,
        "Product Name"                      as product_name,
        "Category Id"                       as category_id,
        "Category Name"                     as category_name,
        "Department Id"                     as department_id,
        "Department Name"                   as department_name,
        "Product Price"                     as product_price,

        -- Order geography (where it shipped to)
        "Order City"                        as order_city,
        "Order State"                       as order_state,
        "Order Country"                     as order_country,
        "Order Region"                      as order_region,
        "Market"                            as market,
        "Latitude"                          as latitude,
        "Longitude"                         as longitude,

        -- Shipping attributes
        "Shipping Mode"                     as shipping_mode,
        "Order Status"                      as order_status,
        "Days for shipping (real)"          as shipping_days_real,
        "Days for shipment (scheduled)"     as shipping_days_scheduled,
        "Delivery Status"                   as delivery_status,
        "Late_delivery_risk"                as late_delivery_risk,

        -- Order financials (measures for the fact table)
        "Type"                              as order_type,
        "Order Item Quantity"               as quantity,
        "Sales"                             as sales,
        "Order Item Discount"               as discount,
        "Order Item Discount Rate"          as discount_rate,
        "Order Item Product Price"          as item_price,
        "Order Item Total"                  as item_total,
        "Benefit per order"                 as benefit_per_order,
        "Order Item Profit Ratio"           as profit_ratio,
        "Order Profit Per Order"            as profit_per_order,
        "Sales per customer"                as sales_per_customer

        -- DROPPED: Customer Password, Customer Street, Product Image,
        -- Product Description, Product Status
        -- Passwords are sensitive data — never belongs in a warehouse
        -- The rest are empty or irrelevant to analytics

    from source
)

select * from cleaned