-- One snapshot row per account per date.
select account_id, snapshot_date, count(*) as n
from {{ ref('fct_account_daily_snapshot') }}
group by account_id, snapshot_date
having count(*) > 1
