-- Posted card transactions at transaction grain, keyed to the dimension versions in
-- force on the transaction date. Incremental: each run adds rows Silver loaded since the
-- last run; a re-delivered transaction replaces its earlier row (delete+insert on txn_id).
{{
    config(
        materialized='incremental',
        unique_key='txn_id',
        incremental_strategy='delete+insert',
    )
}}

with txn as (
    select *
    from {{ ref('stg_silver__posted_transaction') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), timestamptz '1900-01-01') from {{ this }})
    {% endif %}
)

select
    t.txn_id,
    {{ date_key('t.txn_local_date') }} as txn_date_key,
    {{ date_key('t.post_date') }} as post_date_key,
    t.txn_local_date,
    t.post_date,
    t.txn_ts,
    c.customer_sk,
    a.account_sk,
    t.customer_id,
    t.account_id,
    t.card_token,
    t.merchant_id,
    t.mcc,
    t.txn_type_cd,
    t.channel_cd,
    t.amount,
    t.txn_type_cd in ('PURCHASE', 'CASH') as is_spend,
    t._loaded_at
from txn t
left join {{ ref('dim_account') }} a
    on a.account_id = t.account_id and {{ as_of('a', 't.txn_local_date') }}
left join {{ ref('dim_customer') }} c
    on c.customer_id = t.customer_id and {{ as_of('c', 't.txn_local_date') }}
