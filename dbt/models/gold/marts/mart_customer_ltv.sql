{{
    config(
        materialized='table',
        schema='marts',
        tags=['gold', 'mart', 'customer_ltv']
    )
}}

with customer_orders as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.loyalty_tier as current_loyalty_tier,
        c.city,
        min(f.order_timestamp) as first_order_at,
        max(f.order_timestamp) as latest_order_at,
        count(distinct f.order_id) as lifetime_order_count,
        sum(case when f.order_status != 'CANCELLED' then f.net_amount else 0 end) as lifetime_spend
    from {{ ref('dim_customer') }} c
    inner join {{ ref('fact_orders') }} f
        on c.customer_id = f.customer_id
    where c.is_current = true
    group by
        c.customer_id,
        c.full_name,
        c.email,
        c.loyalty_tier,
        c.city
),

metrics as (
    select
        customer_id,
        full_name,
        email,
        current_loyalty_tier,
        city,
        first_order_at,
        latest_order_at,
        date_diff('day', cast(first_order_at as date), current_date) as tenure_days,
        date_diff('day', cast(latest_order_at as date), current_date) as days_since_last_order,
        lifetime_order_count,
        cast(round(lifetime_spend, 2) as decimal(12, 2)) as lifetime_spend,
        case
            when lifetime_order_count > 0 then round(lifetime_spend / lifetime_order_count, 2)
            else 0.0
        end as average_basket_size
    from customer_orders
)

select
    customer_id,
    full_name,
    email,
    current_loyalty_tier,
    city,
    first_order_at,
    latest_order_at,
    tenure_days,
    days_since_last_order,
    lifetime_order_count,
    lifetime_spend,
    average_basket_size,
    case
        when lifetime_spend >= 1500 and days_since_last_order <= 30 then 'Champions'
        when lifetime_spend >= 800 and days_since_last_order <= 60 then 'Loyal Customers'
        when days_since_last_order <= 30 then 'Recent Active'
        when days_since_last_order between 31 and 90 then 'Needs Attention'
        else 'At Risk / Churned'
    end as rfm_segment,
    case
        when days_since_last_order <= 30 then 'LOW'
        when days_since_last_order between 31 and 75 then 'MEDIUM'
        else 'HIGH'
    end as churn_risk_band
from metrics
order by lifetime_spend desc
