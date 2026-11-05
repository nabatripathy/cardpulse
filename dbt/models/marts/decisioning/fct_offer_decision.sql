-- Decision log (Phase 4 fct_offer_decision): one row per fired trigger event, with the outcome
-- (SENT, HOLDOUT, SUPPRESSED), the offer and channel, every suppression code that matched, and
-- the trigger, rule-set and model versions. The audit trail for Compliance and the base for
-- holdout (lift) measurement.
select
    d.decision_id,
    d.trigger_event_id,
    cast({{ date_key('d.business_date') }} as integer) as date_key,
    d.business_date,
    d.trigger_cd,
    d.trigger_version,
    d.customer_id,
    cu.customer_sk,
    d.account_id,
    a.account_sk,
    d.outcome,
    d.offer_code,
    o.offer_sk,
    o.campaign_id,
    d.offer_rank,
    d.channel_cd,
    d.primary_reason_cd,
    d.reason_codes,
    d.offers_evaluated,
    d.holdout_bucket,
    d.outcome = 'HOLDOUT' as is_holdout,
    d.rule_set_version,
    d.model_version,
    d.decided_at
from {{ ref('stg_decision__offer_decision') }} d
left join {{ ref('dim_customer') }} cu
    on cu.customer_id = d.customer_id and {{ as_of('cu', 'd.business_date') }}
left join {{ ref('dim_account') }} a
    on a.account_id = d.account_id and {{ as_of('a', 'd.business_date') }}
left join {{ ref('dim_offer') }} o
    on o.offer_code = d.offer_code and {{ as_of('o', 'd.business_date') }}
