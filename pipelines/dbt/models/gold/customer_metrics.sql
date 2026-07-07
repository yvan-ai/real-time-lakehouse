-- gold.customer_metrics — lifetime value, recency and churn KPIs per customer.
-- SQL port of pipelines/batch/spark-jobs/03_gold_aggregate.py:gold_customer_metrics
-- (the Spark job remains the no-Airflow fallback; keep both in sync).
-- Churn threshold: 90 days, same as lakehouse_common.CHURN_THRESHOLD_DAYS.

{{ config(
    properties={
        "partitioning": "ARRAY['bucket(customer_id, 32)']"
    }
) }}

with customers as (

    select customer_id
    from {{ source('silver', 'customers') }}
    where _source_op in ('c', 'u', 'r')

),

orders as (

    select
        order_id,
        customer_id,
        total_amount,
        created_at
    from {{ source('silver', 'orders') }}
    where
        _source_op in ('c', 'u', 'r')
        and status in ('confirmed', 'shipped', 'delivered', 'completed')

),

order_stats as (

    select
        customer_id,
        count(order_id) as total_orders,
        cast(sum(total_amount) as decimal(18, 2)) as total_revenue,
        cast(sum(total_amount) / count(order_id) as decimal(18, 2)) as avg_order_value,
        min(date(created_at)) as first_order_date,
        max(date(created_at)) as last_order_date
    from orders
    group by customer_id

)

select
    c.customer_id,
    cast(null as varchar) as country_code,
    cast(null as varchar) as segment,
    s.first_order_date,
    s.last_order_date,
    coalesce(s.total_orders, 0) as total_orders,
    coalesce(s.total_revenue, cast(0 as decimal(18, 2))) as total_revenue,
    coalesce(s.avg_order_value, cast(0 as decimal(18, 2))) as avg_order_value,
    cast(date_diff('day', s.last_order_date, current_date) as integer) as days_since_last_order,
    coalesce(date_diff('day', s.last_order_date, current_date) > 90, false) as is_churned,
    cast(current_timestamp as timestamp(6)) as computed_at -- noqa: LT01
from customers as c
left join order_stats as s on c.customer_id = s.customer_id
