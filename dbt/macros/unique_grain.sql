{% test unique_grain(model, columns) %}
-- Un grano compuesto debe ser único y todas sus claves deben estar presentes.
select {{ columns | join(', ') }}, count_big(*) as filas
from {{ model }}
group by {{ columns | join(', ') }}
having count_big(*) > 1
{% for column in columns %}
    or {{ column }} is null
{% endfor %}
{% endtest %}
