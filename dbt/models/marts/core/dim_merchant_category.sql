-- Merchant category codes (Type 1), plus the 9999 "unknown" member used in Silver (DQ-05).
select mcc, mcc_description, category_group
from {{ ref('stg_silver__mcc_reference') }}
union all
select '9999', 'Unknown merchant category', 'UNKNOWN'
