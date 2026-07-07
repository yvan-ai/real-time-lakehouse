-- gold.daily_revenue — daily revenue KPIs per report_date and currency.
-- SQL port of pipelines/batch/spark-jobs/03_gold_aggregate.py:gold_daily_revenue
-- (the Spark job remains the no-Airflow fallback; keep both in sync).

{{ config(
    properties={
        "partitioning": "ARRAY['month(report_date)']"
    }
) }}

with orders as (

    -- Revenue-bearing orders only, last CDC image per row already applied in silver.
    select
        order_id,
        customer_id,
        status,
        total_amount,
        currency,
        created_at
    from {{ source('silver', 'orders') }}
    where
        _source_op in ('c', 'u', 'r')
        and status in ('confirmed', 'shipped', 'delivered', 'completed')

),

order_items as (

    select
        order_id,
        count(*) as item_count,
        cast(sum(line_total) as decimal(18, 2)) as gross_revenue
    from {{ source('silver', 'order_items') }}
    group by order_id

),

first_orders as (

    -- New customers per day: first revenue order lands on report_date.
    select
        first_order_date as report_date,
        count(customer_id) as new_customers
    from (
        select
            customer_id,
            min(date(created_at)) as first_order_date
        from orders
        group by customer_id
    )
    group by first_order_date

),

daily as (

    -- Orders without items fall back to total_amount (same rule as Spark).
    select
        date(o.created_at) as report_date,
        coalesce(o.currency, 'USD') as currency,
        count(o.order_id) as order_count,
        cast(coalesce(sum(oi.item_count), 0) as bigint) as item_count,
        cast(sum(coalesce(oi.gross_revenue, o.total_amount)) as decimal(18, 2)) as gross_revenue,
        cast(sum(coalesce(oi.gross_revenue, o.total_amount)) as decimal(18, 2)) as net_revenue,
        cast(
            sum(coalesce(oi.gross_revenue, o.total_amount)) / count(o.order_id) as decimal(18, 2)
        ) as avg_order_value
    from orders as o
    left join order_items as oi on o.order_id = oi.order_id
    group by date(o.created_at), coalesce(o.currency, 'USD')

)

select
    d.report_date,
    d.currency,
    d.order_count,
    d.item_count,
    d.gross_revenue,
    d.net_revenue,
    d.avg_order_value,
    cast(coalesce(f.new_customers, 0) as bigint) as new_customers,
    cast(current_timestamp as timestamp(6)) as computed_at -- noqa: LT01
from daily as d
left join first_orders as f on d.report_date = f.report_date
