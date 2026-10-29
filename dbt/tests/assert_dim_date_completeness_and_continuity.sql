-- Test: Verify that all order dates in fact_orders exist in dim_date
-- and dim_date has no day gaps between the earliest and latest fact order date.
with order_bounds as (
    select
        min(order_date_key) as min_date,
        max(order_date_key) as max_date
    from {{ ref('fact_orders') }}
),

expected_calendar as (
    select unnest(generate_series(
        (select min_date from order_bounds),
        (select max_date from order_bounds),
        interval '1 day'
    ))::date as expected_date
),

missing_dates as (
    select e.expected_date
    from expected_calendar e
    left join {{ ref('dim_date') }} d
        on e.expected_date = d.date_key
    where d.date_key is null
)

select expected_date
from missing_dates
