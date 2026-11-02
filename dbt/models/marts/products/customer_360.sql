-- Customer 360 data product (contracts/customer_360.yaml): one row per current customer,
-- as of the latest snapshot date. Feeds the eligibility and suppression service
-- (Increment 4) and marketing analytics. Contact history and propensity scores are added
-- in later increments as a minor contract version.
{{ config(contract={'enforced': true}) }}

with as_of as (
    select max(snapshot_date) as as_of_date from {{ ref('fct_account_daily_snapshot') }}
),

customer as (
    select * from {{ ref('dim_customer') }} where is_current
),

accounts as (
    select
        s.customer_id,
        count(*) filter (where s.status_cd = 'OPEN') as open_account_count,
        count(*) as account_count,
        sum(s.credit_limit) filter (where s.status_cd = 'OPEN') as total_credit_limit,
        sum(s.current_balance) as total_current_balance,
        sum(s.revolving_balance) as total_revolving_balance,
        max(s.days_past_due) as max_days_past_due,
        sum(s.spend_30d) as spend_30d,
        sum(s.avg_monthly_spend_6m) as avg_monthly_spend_6m,
        max(s.last_spend_date) as last_spend_date,
        bool_or(s.is_fraud_flagged) as has_fraud_flag,
        bool_or(s.has_acquisition_bonus) as has_acquisition_bonus,
        arg_max(s.product_cd, s.credit_limit) as primary_product_cd
    from {{ ref('fct_account_daily_snapshot') }} s
    where s.snapshot_date = (select as_of_date from as_of)
    group by s.customer_id
),

consent as (
    select
        customer_id,
        bool_or(purpose_cd = 'MARKETING' and channel_cd = 'EMAIL' and consent_status = 'OPT_IN') as email_opt_in,
        bool_or(purpose_cd = 'MARKETING' and channel_cd = 'PUSH' and consent_status = 'OPT_IN') as push_opt_in,
        bool_or(purpose_cd = 'MARKETING' and channel_cd = 'SMS' and consent_status = 'OPT_IN') as sms_opt_in,
        bool_or(purpose_cd = 'MARKETING' and channel_cd = 'MAIL' and consent_status = 'OPT_IN') as mail_opt_in,
        bool_or(purpose_cd = 'DO_NOT_SOLICIT' and consent_status = 'ACTIVE') as is_do_not_solicit
    from {{ ref('stg_silver__marketing_consent') }}
    where _is_current
    group by customer_id
)

select
    cast(c.customer_id as varchar) as customer_id,
    cast(c.customer_sk as varchar) as customer_sk,
    cast(c.first_name as varchar) as first_name,
    cast(c.state_cd as varchar) as state_cd,
    cast(c.zip3 as varchar) as zip3,
    cast(c.segment_cd as varchar) as segment_cd,
    cast(c.customer_since_date as date) as customer_since_date,
    cast(date_diff('month', c.customer_since_date, a0.as_of_date) as integer) as tenure_months,
    cast(year(a0.as_of_date) - c.birth_year as integer) as age_years,
    cast(c.is_deceased as boolean) as is_deceased,
    cast(coalesce(a.account_count, 0) as integer) as account_count,
    cast(coalesce(a.open_account_count, 0) as integer) as open_account_count,
    cast(a.primary_product_cd as varchar) as primary_product_cd,
    cast(coalesce(a.total_credit_limit, 0) as decimal(18, 2)) as total_credit_limit,
    cast(coalesce(a.total_current_balance, 0) as decimal(18, 2)) as total_current_balance,
    cast(coalesce(a.total_revolving_balance, 0) as decimal(18, 2)) as total_revolving_balance,
    cast(case when a.total_credit_limit > 0 then a.total_current_balance / a.total_credit_limit end as double)
        as utilization_ratio,
    cast(coalesce(a.max_days_past_due, 0) as integer) as max_days_past_due,
    cast(coalesce(a.spend_30d, 0) as decimal(18, 2)) as spend_30d,
    cast(coalesce(a.avg_monthly_spend_6m, 0) as decimal(18, 2)) as avg_monthly_spend_6m,
    cast(a.last_spend_date as date) as last_spend_date,
    cast(coalesce(a.has_fraud_flag, false) as boolean) as has_fraud_flag,
    cast(coalesce(a.has_acquisition_bonus, false) as boolean) as has_acquisition_bonus,
    cast(coalesce(n.email_opt_in, false) as boolean) as email_opt_in,
    cast(coalesce(n.push_opt_in, false) as boolean) as push_opt_in,
    cast(coalesce(n.sms_opt_in, false) as boolean) as sms_opt_in,
    cast(coalesce(n.mail_opt_in, false) as boolean) as mail_opt_in,
    cast(coalesce(n.is_do_not_solicit, false) as boolean) as is_do_not_solicit,
    cast(a0.as_of_date as date) as as_of_date
from customer c
cross join as_of a0
left join accounts a using (customer_id)
left join consent n using (customer_id)
