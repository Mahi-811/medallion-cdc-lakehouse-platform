{{
    config(
        materialized='incremental',
        unique_key='order_sk',
        schema='gold',
        tags=['gold', 'fact', 'orders']
    )
}}

with orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_timestamp,
        order_status,
        gross_amount,
        discount_amount,
        tax_amount,
        net_amount
    from {{ ref('silver_orders') }}
    {% if is_incremental() %}
        where order_timestamp >= (select coalesce(max(order_timestamp), '1970-01-01') from {{ this }})
    {% endif %}
),

items as (
    select
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total
    from {{ ref('silver_order_items') }}
),

-- Join with SCD Type 2 Customer Dimension at order timestamp
customers_scd2 as (
    select
        customer_sk,
        customer_id,
        city,
        loyalty_tier,
        valid_from,
        valid_to
    from {{ ref('dim_customer') }}
),

weather as (
    select
        metric_date,
        city,
        temp_mean_c,
        precipitation_mm,
        weather_condition
    from {{ ref('silver_weather') }}
)

select
    -- Primary Surrogate Key for the Fact Record
    md5(concat_ws('||', cast(o.order_id as varchar), cast(coalesce(i.item_id, 0) as varchar))) as order_sk,
    o.order_id,
    coalesce(i.item_id, 0) as item_id,
    
    -- Dimensional Foreign Keys
    coalesce(c.customer_sk, md5(cast(o.customer_id as varchar))) as customer_sk,
    o.customer_id,
    coalesce(i.product_id, 0) as product_id,
    o.order_date as order_date_key,
    
    -- Degenerate Dimensions & Attributes
    o.order_timestamp,
    o.order_status,
    coalesce(c.loyalty_tier, 'BRONZE') as customer_loyalty_tier_at_order,
    coalesce(c.city, 'Unknown') as shipping_city,
    
    -- Transactional Metrics
    coalesce(i.quantity, 1) as quantity,
    coalesce(i.unit_price, o.gross_amount) as unit_price,
    coalesce(i.line_total, o.gross_amount) as gross_amount,
    cast(round(o.discount_amount * (coalesce(i.line_total, o.gross_amount) / nullif(o.gross_amount, 0)), 2) as decimal(12, 2)) as discount_amount,
    cast(round(o.tax_amount * (coalesce(i.line_total, o.gross_amount) / nullif(o.gross_amount, 0)), 2) as decimal(12, 2)) as tax_amount,
    cast(round(o.net_amount * (coalesce(i.line_total, o.gross_amount) / nullif(o.gross_amount, 0)), 2) as decimal(12, 2)) as net_amount,

    -- External Weather Context at Order Time
    w.temp_mean_c as weather_temp_c,
    w.precipitation_mm as weather_precipitation_mm,
    coalesce(w.weather_condition, 'PLEASANT') as weather_condition

from orders o
left join items i
    on o.order_id = i.order_id
left join customers_scd2 c
    on o.customer_id = c.customer_id
    and o.order_timestamp >= c.valid_from
    and o.order_timestamp < c.valid_to
left join weather w
    on o.order_date = w.metric_date
    and c.city = w.city
