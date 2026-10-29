{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'cdc', 'products']
    )
}}

with raw_products as (
    select
        _loaded_at,
        _source,
        _batch_id,
        _cdc_op,
        _cdc_ts_ms,
        cast(product_id as bigint) as product_id,
        upper(trim(sku)) as sku,
        trim(product_name) as product_name,
        trim(category) as category,
        cast(unit_cost as decimal(10, 2)) as unit_cost,
        cast(list_price as decimal(10, 2)) as list_price,
        cast(stock_quantity as bigint) as stock_quantity,
        cast(supplier_id as bigint) as supplier_id,
        cast(is_active as boolean) as is_active,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        row_number() over (
            partition by product_id
            order by _cdc_ts_ms desc, _loaded_at desc
        ) as rn
    from {{ source('bronze', 'bronze_products_cdc') }}
),

latest_supplier_manifest as (
    select
        supplier_id,
        trim(supplier_name) as supplier_name,
        upper(trim(product_sku)) as product_sku,
        cast(unit_cost as decimal(10, 2)) as supplier_unit_cost,
        cast(available_stock as bigint) as supplier_available_stock,
        cast(replenishment_lead_days as bigint) as replenishment_lead_days,
        cast(restock_date as date) as restock_date,
        row_number() over (
            partition by product_sku
            order by restock_date desc, _loaded_at desc
        ) as rn
    from {{ source('bronze', 'bronze_supplier_inventory_csv') }}
)

select
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    coalesce(s.supplier_unit_cost, p.unit_cost) as unit_cost,
    p.list_price,
    coalesce(s.supplier_available_stock, p.stock_quantity) as available_stock,
    p.supplier_id,
    coalesce(s.supplier_name, concat('Supplier-', cast(p.supplier_id as varchar))) as supplier_name,
    coalesce(s.replenishment_lead_days, 7) as replenishment_lead_days,
    s.restock_date as latest_restock_date,
    p.is_active,
    p.updated_at
from raw_products p
left join latest_supplier_manifest s
    on p.sku = s.product_sku and s.rn = 1
where p.rn = 1 and p._cdc_op != 'd'
