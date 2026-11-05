select
    contact_id,
    customer_id,
    account_id,
    campaign_id,
    offer_cd as offer_code,
    trigger_cd,
    channel_cd,
    purpose_cd,
    contact_ts,
    contact_date
from {{ source('silver', 'offer_contact') }}
