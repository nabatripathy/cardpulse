-- Messages delivered to customers (marketing and service), one row per contact.
-- Feeds the contact cap (S08), the repeat-offer rule (S09) and the trigger repeat rules.
select
    c.contact_id,
    cast({{ date_key('c.contact_date') }} as integer) as date_key,
    c.contact_date,
    c.contact_ts,
    c.customer_id,
    cu.customer_sk,
    nullif(c.account_id, '') as account_id,
    c.campaign_id,
    c.offer_code,
    nullif(c.trigger_cd, '') as trigger_cd,
    c.channel_cd,
    c.purpose_cd
from {{ ref('stg_silver__offer_contact') }} c
left join {{ ref('dim_customer') }} cu
    on cu.customer_id = c.customer_id and {{ as_of('cu', 'c.contact_date') }}
