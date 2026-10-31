{#- Generic test: every row satisfies a SQL expression (optionally filtered). -#}
{% test expression_is_true(model, expression, where=None) %}
select * from {{ model }}
where not coalesce(({{ expression }}), false)
{% if where %} and ({{ where }}) {% endif %}
{% endtest %}
