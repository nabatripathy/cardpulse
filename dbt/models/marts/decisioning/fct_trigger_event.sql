-- Every account that met a trigger's conditions on a business date, and whether the trigger
-- fired or was held back by its repeat rules (max fires, minimum gap).
select
    e.trigger_event_id,
    cast({{ date_key('e.business_date') }} as integer) as date_key,
    e.business_date,
    e.trigger_cd,
    e.trigger_version,
    e.account_id,
    a.account_sk,
    e.customer_id,
    e.status,
    e.prior_fires,
    e.last_fire_date,
    e.inputs,
    e.evaluated_at
from {{ ref('stg_decision__trigger_event') }} e
left join {{ ref('dim_account') }} a
    on a.account_id = e.account_id and {{ as_of('a', 'e.business_date') }}
