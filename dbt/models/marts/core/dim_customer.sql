-- Customer dimension, SCD Type 2 (history comes from Silver; one row per customer version).
with versions as (
    select
        *,
        {{ scd_valid_from('customer_id') }} as valid_from
    from {{ ref('stg_silver__customer') }}
)

select
    _sk as customer_sk,
    customer_id,
    first_name,
    last_name,
    birth_year,
    state_cd,
    zip3,
    segment_cd,
    customer_since_date,
    is_deceased,
    valid_from,
    _valid_to as valid_to,
    _is_current as is_current
from versions
