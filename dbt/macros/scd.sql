{#- Helpers for SCD Type 2 dimensions built on Silver history. -#}

{#- Effective-from for a dimension version: the first version of each key starts at the
    beginning of time, so facts older than the first snapshot still join. -#}
{% macro scd_valid_from(key) -%}
    case
        when row_number() over (partition by {{ key }} order by _valid_from) = 1
            then cast('{{ var("scd_beginning_of_time") }}' as date)
        else _valid_from
    end
{%- endmacro %}

{#- As-of join condition: the dimension version in force on date `on_date`
    (valid_to is exclusive). -#}
{% macro as_of(dim_alias, on_date) -%}
    {{ dim_alias }}.valid_from <= {{ on_date }} and {{ on_date }} < {{ dim_alias }}.valid_to
{%- endmacro %}

{#- Integer date key YYYYMMDD. -#}
{% macro date_key(d) -%}
    cast(strftime({{ d }}, '%Y%m%d') as integer)
{%- endmacro %}
