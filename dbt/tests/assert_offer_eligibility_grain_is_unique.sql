-- Offer Eligibility grain: one row per customer and offer.
{{ config(tags=['decisioning']) }}
select customer_id, offer_code, count(*) as n
from {{ ref('offer_eligibility') }}
group by customer_id, offer_code
having count(*) > 1
