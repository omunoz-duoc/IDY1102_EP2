-- Un documento, una campaña; la prueba asociada rechaza campañas conflictivas.
select documento_nk, max(campania_key) as campania_key
from {{ source('warehouse', 'fact_ventas') }}
group by documento_nk
