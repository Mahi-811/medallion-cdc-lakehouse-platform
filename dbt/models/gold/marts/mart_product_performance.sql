{{
    config(
        materialized='table',
        schema='marts',
        tags=['gold', 'mart', 'products']
    )
}}

with product_aggregates as (
    select
        p.product_id,
        p.sku,
        p.product_name,
        p.category,
        p.unit_cost,
        p.list_price,
        p.available_stock,
        p.supplier_name,
        count(distinct f.order_id) as total_order_appearances,
        sum(f.quantity) as total_units_sold,
        sum(case when f.order_status = 'CANCELLED' then f.quantity else 0 end) as units_cancelled,
        sum(case when f.order_status != 'CANCELLED' then f.gross_amount else 0 end) as total_gross_revenue,
        sum(case when f.order_status != 'CANCELLED' then f.net_amount else 0 end) as total_net_revenue,
        sum(case when f.order_status != 'CANCELLED' then (f.quantity * p.unit_cost) else 0 end) as total_cogs
    from {{ ref('dim_product') }} p
    left join {{ ref('fact_orders') }} f
        on p.product_id = f.product_id
    group by
        p.product_id,
        p.sku,
        p.product_name,
        p.category,
        p.unit_cost,
        p.list_price,
        p.available_stock,
        p.supplier_name
)

select
    product_id,
    sku,
    product_name,
    category,
    unit_cost,
    list_price,
    available_stock,
    supplier_name,
    coalesce(total_order_appearances, 0) as total_order_appearances,
    coalesce(total_units_sold, 0) as total_units_sold,
    coalesce(units_cancelled, 0) as units_cancelled,
    cast(round(coalesce(total_gross_revenue, 0), 2) as decimal(12, 2)) as total_gross_revenue,
    cast(round(coalesce(total_net_revenue, 0), 2) as decimal(12, 2)) as total_net_revenue,
    cast(round(coalesce(total_net_revenue, 0) - coalesce(total_cogs, 0), 2) as decimal(12, 2)) as total_gross_margin,
    case
        when total_units_sold > 0 then round((coalesce(units_cancelled, 0) * 100.0) / total_units_sold, 2)
        else 0.0
    end as cancellation_rate_pct,
    dense_rank() over (order by coalesce(total_net_revenue, 0) desc) as revenue_rank_overall,
    dense_rank() over (partition by category order by coalesce(total_net_revenue, 0) desc) as revenue_rank_within_category
from product_aggregates
order by revenue_rank_overall asc
