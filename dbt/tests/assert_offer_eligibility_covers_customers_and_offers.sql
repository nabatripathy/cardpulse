-- Offer Eligibility has a row for every current customer and every offer active on the date.
{{ config(tags=['decisioning']) }}
with expected as (
    select count(*) * (
        select count(*) from {{ ref('dim_offer') }}
        where is_current
          and (select max(business_date) from {{ ref('stg_decision__offer_eligibility') }})
              between start_date and end_date
    ) as n
    from {{ ref('customer_360') }}
)
select expected.n, actual.n
from expected, (select count(*) as n from {{ ref('offer_eligibility') }}) actual
where expected.n <> actual.n
