{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        schema='silver',
        tags=['silver', 'cdc', 'customers']
    )
}}

with raw_cust as (
    select
        _loaded_at,
        _source,
        _batch_id,
        _cdc_op,
        _cdc_ts_ms,
        cast(customer_id as bigint) as customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        trim(phone) as phone,
        trim(address) as address,
        trim(city) as city,
        upper(trim(loyalty_tier)) as loyalty_tier,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        row_number() over (
            partition by customer_id
            order by _cdc_ts_ms desc, _loaded_at desc
        ) as rn
    from {{ source('bronze', 'bronze_customers_cdc') }}
    {% if is_incremental() %}
        where _loaded_at >= (select coalesce(max(_loaded_at), '1970-01-01') from {{ this }})
    {% endif %}
)

select
    _loaded_at,
    _source,
    _batch_id,
    customer_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,
    email,
    phone,
    address,
    city,
    coalesce(loyalty_tier, 'BRONZE') as loyalty_tier,
    created_at,
    updated_at,
    case when _cdc_op = 'd' then true else false end as is_deleted
from raw_cust
where rn = 1
