-- ref fija la dependencia: este control precede al modelo de Marketing.
select i.documento_nk
from {{ ref('int_documentos_campania') }} i
join {{ source('warehouse', 'fact_ventas') }} f on i.documento_nk=f.documento_nk
group by i.documento_nk
having count(distinct f.campania_key) > 1
