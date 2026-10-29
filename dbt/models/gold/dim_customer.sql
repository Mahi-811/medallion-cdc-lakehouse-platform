{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'scd2', 'customers']
    )
}}

with customer_cdc_history as (
    select
        cast(customer_id as bigint) as customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        concat(trim(first_name), ' ', trim(last_name)) as full_name,
        lower(trim(email)) as email,
        trim(phone) as phone,
        trim(address) as address,
        trim(city) as city,
        upper(trim(coalesce(loyalty_tier, 'BRONZE'))) as loyalty_tier,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        _cdc_op,
        _cdc_ts_ms,
        _loaded_at
    from {{ source('bronze', 'bronze_customers_cdc') }}
),

-- Deduplicate identical mutations in the same millisecond
deduped_mutations as (
    select
        *,
        row_number() over (
            partition by customer_id, updated_at
            order by _cdc_ts_ms desc, _loaded_at desc
        ) as rn
    from customer_cdc_history
),

-- Order changes chronologically to construct SCD2 windows
ordered_changes as (
    select
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        address,
        city,
        loyalty_tier,
        updated_at,
        row_number() over (
            partition by customer_id
            order by updated_at asc, _cdc_ts_ms asc
        ) as version_num
    from deduped_mutations
    where rn = 1 and _cdc_op != 'd'
),

scd2_windows as (
    select
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        address,
        city,
        loyalty_tier,
        version_num,
        -- The first version starts at created_at, subsequent versions start at updated_at
        updated_at as valid_from,
        lead(updated_at, 1, timestamp '9999-12-31 23:59:59') over (
            partition by customer_id
            order by updated_at asc
        ) as valid_to
    from ordered_changes
)

select
    md5(concat_ws('||', cast(customer_id as varchar), cast(valid_from as varchar))) as customer_sk,
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    address,
    city,
    loyalty_tier,
    version_num,
    valid_from,
    valid_to,
    case when valid_to = timestamp '9999-12-31 23:59:59' then true else false end as is_current
from scd2_windows
