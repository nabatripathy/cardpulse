select * from {{ source('decision', 'offer_eligibility') }}
