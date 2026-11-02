-- DQ-15 (local form): the snapshot carries every Silver balance row and the same total
-- receivables. In production this also ties out to the general ledger feed.
with silver as (
    select balance_date, count(*) as n, sum(current_balance) as receivables
    from {{ ref('stg_silver__account_daily_balance') }} group by balance_date
),
gold as (
    select snapshot_date as balance_date, count(*) as n, sum(current_balance) as receivables
    from {{ ref('fct_account_daily_snapshot') }} group by snapshot_date
)
select s.balance_date, s.n as silver_rows, g.n as gold_rows, s.receivables as silver_receivables,
       g.receivables as gold_receivables
from silver s full join gold g using (balance_date)
where s.n is distinct from g.n or s.receivables is distinct from g.receivables
