-- No two versions of a member are valid on the same day (as-of joins return one row).
select 'dim_account' as dim, a.account_id as id
from {{ ref('dim_account') }} a
join {{ ref('dim_account') }} b
    on a.account_id = b.account_id and a.account_sk <> b.account_sk
    and a.valid_from < b.valid_to and b.valid_from < a.valid_to
union all
select 'dim_customer', a.customer_id
from {{ ref('dim_customer') }} a
join {{ ref('dim_customer') }} b
    on a.customer_id = b.customer_id and a.customer_sk <> b.customer_sk
    and a.valid_from < b.valid_to and b.valid_from < a.valid_to
