-- Offer Eligibility data product (contracts/offer_eligibility.yaml): one row per customer and
-- active offer stating whether the customer may receive it now, and if not, why (S01-S12).
{{ config(contract={'enforced': true}, tags=['data_product']) }}

select
    cast(customer_id as varchar) as customer_id,
    cast(offer_code as varchar) as offer_code,
    cast(is_eligible as boolean) as is_eligible,
    cast(suppression_codes as varchar[]) as suppression_codes,
    cast(last_contact_ts as timestamp with time zone) as last_contact_ts,
    cast(contacts_30d as integer) as contacts_30d,
    cast(rule_set_version as varchar) as rule_set_version,
    cast(evaluated_at as timestamp with time zone) as evaluated_at
from {{ ref('stg_decision__offer_eligibility') }}
