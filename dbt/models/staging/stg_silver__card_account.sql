-- All versions of silver.card_account (SCD Type 2). Business columns pass through; the Silver
-- audit columns are renamed to the dimension conventions (valid_to is exclusive).
select
    * exclude (_sk, _record_hash, _valid_from, _valid_to, _is_current, _bronze_batch_id, _source_file,
               _business_date, _silver_run_id, _loaded_at),
    _sk,
    _valid_from,
    _valid_to,
    _is_current
from {{ source('silver', 'card_account') }}
