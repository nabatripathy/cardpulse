-- Gold transactions match Silver exactly: same count and same total amount.
with silver as (
    select count(*) as n, sum(amount) as total from {{ ref('stg_silver__posted_transaction') }}
),
gold as (
    select count(*) as n, sum(amount) as total from {{ ref('fct_transaction') }}
)
select silver.n as silver_rows, gold.n as gold_rows, silver.total as silver_amount, gold.total as gold_amount
from silver, gold
where silver.n <> gold.n or silver.total <> gold.total
