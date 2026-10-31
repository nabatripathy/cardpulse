select
    txn_id,
    card_token,
    account_id,
    customer_id,
    post_date,
    txn_local_date,
    txn_ts,
    amount,
    currency_cd,
    merchant_id,
    merchant_name,
    mcc,
    txn_type_cd,
    channel_cd,
    _loaded_at
from {{ source('silver', 'posted_transaction') }}
