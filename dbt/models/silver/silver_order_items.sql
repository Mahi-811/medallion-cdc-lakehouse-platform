{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'cdc', 'order_items']
    )
}}

with raw_items as (
    select
        _loaded_at,
        _source,
        _batch_id,
        _cdc_op,
        _cdc_ts_ms,
        cast(item_id as bigint) as item_id,
        cast(order_id as bigint) as order_id,
        cast(product_id as bigint) as product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as decimal(10, 2)) as unit_price,
        cast(line_total as decimal(10, 2)) as line_total,
        cast(created_at as timestamp) as created_at,
        row_number() over (
            partition by item_id
            order by _cdc_ts_ms desc, _loaded_at desc
        ) as rn
    from {{ source('bronze', 'bronze_order_items_cdc') }}
)

select
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    created_at
from raw_items
where rn = 1 and _cdc_op != 'd'
