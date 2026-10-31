select
    account_id,
    balance_date,
    statement_balance,
    current_balance,
    revolving_balance,
    available_credit,
    days_past_due
from {{ source('silver', 'account_daily_balance') }}
