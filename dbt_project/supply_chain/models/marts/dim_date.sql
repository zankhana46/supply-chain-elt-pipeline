/*
    Date dimension: one row per unique date.
    
    This is a standard calendar table found in every data warehouse.
    It lets analysts filter/group by year, quarter, month, day of week
    without writing date functions in every query.
    
    We generate it from the actual dates in our data rather than
    a full calendar range — keeps it simple for this project.
*/

with all_dates as (
    -- Combine both order dates and shipping dates
    select distinct cast(order_date as date) as date_key
    from {{ ref('stg_supply_chain') }}
    
    union
    
    select distinct cast(shipping_date as date) as date_key
    from {{ ref('stg_supply_chain') }}
)

select
    date_key,
    year(date_key)                      as year,
    quarter(date_key)                   as quarter,
    month(date_key)                     as month,
    monthname(date_key)                 as month_name,
    day(date_key)                       as day,
    dayofweek(date_key)                 as day_of_week,
    dayname(date_key)                   as day_name,
    weekofyear(date_key)                as week_of_year,
    -- Is it a weekend? Useful for shipping analysis
    case when dayofweek(date_key) in (0, 6) 
        then true else false 
    end as is_weekend
from all_dates
where date_key is not null