{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'products']
    )
}}

select
    product_id,
    sku,
    product_name,
    category,
    unit_cost,
    list_price,
    cast(list_price - unit_cost as decimal(10, 2)) as gross_margin_amount,
    case
        when list_price > 0 then round(((list_price - unit_cost) / list_price) * 100.0, 2)
        else 0.0
    end as gross_margin_pct,
    available_stock,
    supplier_id,
    supplier_name,
    replenishment_lead_days,
    latest_restock_date,
    is_active,
    updated_at as last_catalog_sync_at
from {{ ref('silver_products') }}

union all

-- Kimball standard: Unknown/Default dimensional member for referential integrity
select
    0 as product_id,
    'SKU-DEFAULT' as sku,
    'General Merchandise / Unassigned' as product_name,
    'General' as category,
    cast(0.00 as decimal(10, 2)) as unit_cost,
    cast(0.00 as decimal(10, 2)) as list_price,
    cast(0.00 as decimal(10, 2)) as gross_margin_amount,
    0.0 as gross_margin_pct,
    0 as available_stock,
    0 as supplier_id,
    'Internal Warehouse' as supplier_name,
    0 as replenishment_lead_days,
    cast('2024-01-01' as date) as latest_restock_date,
    true as is_active,
    timestamp '2024-01-01 00:00:00' as last_catalog_sync_at
