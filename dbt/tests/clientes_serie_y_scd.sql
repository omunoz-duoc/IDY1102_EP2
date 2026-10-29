with series as (
    select cliente_nk,count_big(*) n,
        datediff(month,min(datefromparts(anio,mes,1)),max(datefromparts(anio,mes,1)))+1 esperado,
        max(fecha_corte_key) corte from {{ ref('clientes_mensuales') }} group by cliente_nk
)
select cliente_nk from series where n<>esperado or corte<>{{ var('customer_cutoff_key') | int }}
union all
select m.cliente_nk from {{ ref('clientes_mensuales') }} m
join {{ source('warehouse','dim_cliente') }} c on m.cliente_key_corte=c.cliente_key
join {{ source('warehouse','dim_fecha') }} d on m.fecha_corte_key=d.fecha_key
where m.cliente_nk='DESCONOCIDO' or m.fecha_corte_key>{{ var('customer_cutoff_key') | int }}
    or (m.cliente_key_corte<>0 and (m.cliente_nk<>c.cliente_nk
        or not(c.valido_desde<dateadd(day,1,d.fecha) and c.valido_hasta>=dateadd(day,1,d.fecha))))
    or m.sin_version_al_corte<>case when m.cliente_key_corte=0 then 1 else 0 end
