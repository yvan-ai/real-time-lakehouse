-- Grain check: exactly one row per (report_date, currency).
-- dbt-core has no multi-column unique generic test without dbt_utils, and the
-- cluster's flaky egress makes package installs undesirable — hence this
-- singular test.
select
    report_date,
    currency,
    count(*) as row_count
from {{ ref('daily_revenue') }}
group by 1, 2
having count(*) > 1
