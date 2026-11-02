-- Offer dimension from Compliance-approved campaigns, SCD Type 2.
with versions as (
    select
        *,
        {{ scd_valid_from('campaign_id') }} as valid_from
    from {{ ref('stg_silver__campaign') }}
)

select
    _sk as offer_sk,
    offer_cd as offer_code,
    campaign_id,
    campaign_name,
    trigger_cd,
    offer_value,
    est_cost_usd,
    start_date,
    end_date,
    compliance_approval_id,
    product_scope,
    valid_from,
    _valid_to as valid_to,
    _is_current as is_current
from versions
