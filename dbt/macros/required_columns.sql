{% test required_columns(model, columns) %}
select * from {{ model }} where
{% for column in columns %}
    {{ column }} is null {% if not loop.last %} or {% endif %}
{% endfor %}
{% endtest %}

{% test not_empty(model) %}
select count_big(*) as filas from {{ model }} having count_big(*) = 0
{% endtest %}
