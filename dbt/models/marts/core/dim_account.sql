-- Card account dimension, SCD Type 2. The primary holder is the account's current
-- primary customer (roles do not change in the synthetic portfolio; see README).
with versions as (
    select
        *,
        {{ scd_valid_from('account_id') }} as valid_from
    from {{ ref('stg_silver__card_account') }}
),

primary_holder as (
    select account_id, min(customer_id) as primary_customer_id
    from {{ ref('stg_silver__customer_account_role') }}
    where _is_current and role_cd = 'PRIMARY'
    group by account_id
)

select
    v._sk as account_sk,
    v.account_id,
    ph.primary_customer_id,
    v.product_cd,
    v.status_cd,
    v.open_date,
    v.close_date,
    v.credit_limit,
    case
        when v.credit_limit is null then 'UNKNOWN'
        when v.credit_limit < 2500 then 'UNDER_2.5K'
        when v.credit_limit < 5000 then '2.5K_5K'
        when v.credit_limit < 10000 then '5K_10K'
        when v.credit_limit < 25000 then '10K_25K'
        else '25K_PLUS'
    end as credit_limit_band,
    v.apr_bps,
    v.cycle_day,
    v.is_fraud_flagged,
    v.has_acquisition_bonus,
    v.valid_from,
    v._valid_to as valid_to,
    v._is_current as is_current
from versions v
left join primary_holder ph using (account_id)
