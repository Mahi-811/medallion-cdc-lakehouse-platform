{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'date']
    )
}}

with date_spine as (
    -- Generates 5 years of daily calendar dates (2024-01-01 to 2028-12-31)
    select unnest(generate_series(
        date '2024-01-01',
        date '2028-12-31',
        interval '1 day'
    ))::date as date_day
)

select
    date_day as date_key,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    strftime(date_day, '%B') as month_name,
    strftime(date_day, '%b') as month_short,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week_num,
    strftime(date_day, '%A') as day_name,
    case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend,
    case
        when (extract(month from date_day) = 1 and extract(day from date_day) = 1) then true   -- New Year
        when (extract(month from date_day) = 7 and extract(day from date_day) = 4) then true   -- Independence Day
        when (extract(month from date_day) = 12 and extract(day from date_day) = 25) then true -- Christmas
        else false
    end as is_holiday,
    -- Fiscal Year (Starts February 1st)
    case
        when extract(month from date_day) >= 2 then extract(year from date_day)
        else extract(year from date_day) - 1
    end as fiscal_year,
    case
        when extract(month from date_day) in (2, 3, 4) then 1
        when extract(month from date_day) in (5, 6, 7) then 2
        when extract(month from date_day) in (8, 9, 10) then 3
        else 4
    end as fiscal_quarter
from date_spine
