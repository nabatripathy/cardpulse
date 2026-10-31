-- Card Account Snapshot data product (contracts/card_account_snapshot.yaml).
-- Grain: account x snapshot date. Balances come from the processor's daily file;
-- spend measures are computed from transactions up to and including the snapshot date.
-- These are the inputs to the MVP triggers: T01 (card issued 10-60 days ago, no purchase)
-- and T02 (30-day spend below half the 6-month average).
{{ config(contract={'enforced': true}) }}

with balance as (
    select * from {{ ref('stg_silver__account_daily_balance') }}
),

spend_txn as (
    select account_id, txn_local_date, amount, txn_type_cd
    from {{ ref('stg_silver__posted_transaction') }}
    where txn_type_cd in ('PURCHASE', 'CASH')
),

-- First date with transactions on record: the 6-month average only divides by history we hold.
coverage as (
    select min(txn_local_date) as data_start from {{ ref('stg_silver__posted_transaction') }}
),

spend as (
    select
        b.account_id,
        b.balance_date,
        coalesce(sum(t.amount) filter (where t.txn_local_date > b.balance_date - 30), 0) as spend_30d,
        count(t.amount) filter (where t.txn_local_date > b.balance_date - 30) as txn_count_30d,
        coalesce(sum(t.amount), 0) as spend_180d,
        max(t.txn_local_date) as last_spend_date
    from balance b
    left join spend_txn t
        on t.account_id = b.account_id
        and t.txn_local_date > b.balance_date - 180
        and t.txn_local_date <= b.balance_date
    group by b.account_id, b.balance_date
),

lifetime as (
    select
        b.account_id,
        b.balance_date,
        count(t.amount) filter (where t.txn_type_cd = 'PURCHASE') as lifetime_purchase_count,
        min(t.txn_local_date) filter (where t.txn_type_cd = 'PURCHASE') as first_purchase_date
    from balance b
    left join spend_txn t
        on t.account_id = b.account_id and t.txn_local_date <= b.balance_date
    group by b.account_id, b.balance_date
),

cards as (
    select
        b.account_id,
        b.balance_date,
        min(c.issue_date) as first_card_issue_date,
        coalesce(bool_or(c.activation_date <= b.balance_date), false) as is_card_activated
    from balance b
    left join {{ ref('stg_silver__card') }} c
        on c._is_current and c.account_id = b.account_id
    group by b.account_id, b.balance_date
),

joined as (
    select
        b.*,
        a.account_sk,
        a.primary_customer_id,
        a.product_cd,
        a.status_cd,
        a.open_date,
        a.credit_limit,
        a.is_fraud_flagged,
        a.has_acquisition_bonus,
        cu.customer_sk,
        s.spend_30d,
        s.txn_count_30d,
        s.spend_180d,
        s.last_spend_date,
        l.lifetime_purchase_count,
        l.first_purchase_date,
        cd.first_card_issue_date,
        cd.is_card_activated,
        date_diff('month', a.open_date, b.balance_date) as months_on_book,
        -- Days of spend history behind the 6-month average: at most 180, never before the
        -- account opened or before the first transaction on record, and at least 30.
        greatest(30, least(180, b.balance_date - a.open_date, b.balance_date - cov.data_start + 1)) as avg_window_days
    from balance b
    cross join coverage cov
    left join {{ ref('dim_account') }} a
        on a.account_id = b.account_id and {{ as_of('a', 'b.balance_date') }}
    left join {{ ref('dim_customer') }} cu
        on cu.customer_id = a.primary_customer_id and {{ as_of('cu', 'b.balance_date') }}
    left join spend s on s.account_id = b.account_id and s.balance_date = b.balance_date
    left join lifetime l on l.account_id = b.account_id and l.balance_date = b.balance_date
    left join cards cd on cd.account_id = b.account_id and cd.balance_date = b.balance_date
)

select
    cast({{ date_key('balance_date') }} as integer) as date_key,
    cast(balance_date as date) as snapshot_date,
    cast(account_sk as varchar) as account_sk,
    cast(customer_sk as varchar) as customer_sk,
    cast(account_id as varchar) as account_id,
    cast(primary_customer_id as varchar) as customer_id,
    cast(product_cd as varchar) as product_cd,
    cast(status_cd as varchar) as status_cd,
    cast(open_date as date) as open_date,
    cast(months_on_book as integer) as months_on_book,
    cast(credit_limit as decimal(18, 2)) as credit_limit,
    cast(statement_balance as decimal(18, 2)) as statement_balance,
    cast(current_balance as decimal(18, 2)) as current_balance,
    cast(revolving_balance as decimal(18, 2)) as revolving_balance,
    cast(available_credit as decimal(18, 2)) as available_credit,
    cast(case when credit_limit > 0 then current_balance / credit_limit end as double) as utilization_ratio,
    cast(days_past_due as integer) as days_past_due,
    cast(coalesce(days_past_due, 0) > 0 as boolean) as is_delinquent,
    cast(spend_30d as decimal(18, 2)) as spend_30d,
    cast(txn_count_30d as integer) as txn_count_30d,
    -- Average monthly spend over the last 6 months (180 days), or over the shorter window of
    -- history available (account age, or transactions on record), in 30.4-day months.
    cast(spend_180d / (avg_window_days / 30.4375) as decimal(18, 2)) as avg_monthly_spend_6m,
    cast(last_spend_date as date) as last_spend_date,
    cast(balance_date - last_spend_date as integer) as days_since_last_spend,
    cast(lifetime_purchase_count as integer) as lifetime_purchase_count,
    cast(first_purchase_date as date) as first_purchase_date,
    cast(first_card_issue_date as date) as first_card_issue_date,
    cast(balance_date - first_card_issue_date as integer) as days_since_card_issue,
    cast(is_card_activated as boolean) as is_card_activated,
    cast(is_fraud_flagged as boolean) as is_fraud_flagged,
    cast(has_acquisition_bonus as boolean) as has_acquisition_bonus
from joined
