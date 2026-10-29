-- Test: Verify that no customer has overlapping validity intervals in SCD Type 2 dimension.
select
    a.customer_id,
    a.customer_sk as sk_a,
    b.customer_sk as sk_b,
    a.valid_from as a_valid_from,
    a.valid_to as a_valid_to,
    b.valid_from as b_valid_from,
    b.valid_to as b_valid_to
from {{ ref('dim_customer') }} a
inner join {{ ref('dim_customer') }} b
    on a.customer_id = b.customer_id
    and a.customer_sk != b.customer_sk
    and a.valid_from < b.valid_to
    and a.valid_to > b.valid_from
