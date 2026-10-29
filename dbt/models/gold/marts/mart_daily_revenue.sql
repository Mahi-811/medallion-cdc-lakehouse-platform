{{
    config(
        materialized='table',
        schema='marts',
        tags=['gold', 'mart', 'revenue']
    )
}}

with daily_orders as (
    select
        order_date_key as report_date,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as distinct_customers,
        count(distinct case when order_status = 'CANCELLED' then order_id end) as cancelled_orders,
        count(distinct case when order_status = 'DELIVERED' then order_id end) as delivered_orders,
        sum(case when order_status != 'CANCELLED' then gross_amount else 0 end) as gross_revenue,
        sum(case when order_status != 'CANCELLED' then discount_amount else 0 end) as total_discounts,
        sum(case when order_status != 'CANCELLED' then tax_amount else 0 end) as total_tax,
        sum(case when order_status != 'CANCELLED' then net_amount else 0 end) as net_revenue
    from {{ ref('fact_orders') }}
    group by order_date_key
)

select
    report_date,
    total_orders,
    distinct_customers,
    cancelled_orders,
    delivered_orders,
    cast(round(gross_revenue, 2) as decimal(12, 2)) as gross_revenue,
    cast(round(total_discounts, 2) as decimal(12, 2)) as total_discounts,
    cast(round(total_tax, 2) as decimal(12, 2)) as total_tax,
    cast(round(net_revenue, 2) as decimal(12, 2)) as net_revenue,
    case
        when total_orders > 0 then round(net_revenue / nullif(total_orders - cancelled_orders, 0), 2)
        else 0.0
    end as average_order_value,
    case
        when total_orders > 0 then round((cancelled_orders * 100.0) / total_orders, 2)
        else 0.0
    end as cancellation_rate_pct
from daily_orders
order by report_date desc
