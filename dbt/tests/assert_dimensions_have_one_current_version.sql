-- Every dimension member has exactly one current version.
select 'dim_customer' as dim, customer_id as id, count(*) filter (where is_current) as current_versions
from {{ ref('dim_customer') }} group by customer_id having count(*) filter (where is_current) <> 1
union all
select 'dim_account', account_id, count(*) filter (where is_current)
from {{ ref('dim_account') }} group by account_id having count(*) filter (where is_current) <> 1
