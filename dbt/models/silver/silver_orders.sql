{{
    config(
        materialized='incremental',
        unique_key='order_id',
        schema='silver',
        tags=['silver', 'cdc', 'orders']
    )
}}

with raw_cdc as (
    select
        _loaded_at,
        _source,
        _batch_id,
        _cdc_op,
        _cdc_ts_ms,
        cast(order_id as bigint) as order_id,
        cast(customer_id as bigint) as customer_id,
        cast(order_date as date) as order_date,
        cast(order_timestamp as timestamp) as order_timestamp,
        upper(trim(order_status)) as order_status,
        cast(gross_amount as decimal(12, 2)) as gross_amount,
        cast(discount_amount as decimal(12, 2)) as discount_amount,
        cast(tax_amount as decimal(12, 2)) as tax_amount,
        cast(net_amount as decimal(12, 2)) as net_amount,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        row_number() over (
            partition by order_id
            order by _cdc_ts_ms desc, _loaded_at desc
        ) as rn
    from {{ source('bronze', 'bronze_orders_cdc') }}
    {% if is_incremental() %}
        where _loaded_at >= (select coalesce(max(_loaded_at), '1970-01-01') from {{ this }})
    {% endif %}
),

latest_state as (
    select
        _loaded_at,
        _source,
        _batch_id,
        _cdc_op,
        _cdc_ts_ms,
        order_id,
        customer_id,
        order_date,
        order_timestamp,
        order_status,
        gross_amount,
        discount_amount,
        tax_amount,
        net_amount,
        created_at,
        updated_at,
        case when _cdc_op = 'd' then true else false end as is_deleted
    from raw_cdc
    where rn = 1
)

select
    _loaded_at,
    _source,
    _batch_id,
    order_id,
    customer_id,
    order_date,
    order_timestamp,
    order_status,
    gross_amount,
    discount_amount,
    tax_amount,
    net_amount,
    created_at,
    updated_at,
    is_deleted
from latest_state
where is_deleted = false
