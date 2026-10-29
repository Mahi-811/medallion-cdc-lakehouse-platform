-- Test: Verify that all order lines have non-negative net amounts
-- and that gross_amount - discount_amount + tax_amount matches net_amount within rounding tolerance.
select
    order_sk,
    order_id,
    gross_amount,
    discount_amount,
    tax_amount,
    net_amount,
    abs((gross_amount - discount_amount + tax_amount) - net_amount) as diff
from {{ ref('fact_orders') }}
where net_amount < 0
   or abs((gross_amount - discount_amount + tax_amount) - net_amount) > 0.05
