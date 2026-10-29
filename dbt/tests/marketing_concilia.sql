with m as (
    select sum(envios) envios,sum(sesiones) sesiones,sum(documentos_asociados) documentos,
        sum(venta_neta_asociada_clp) neto from {{ ref('campanias_resumen') }}
)
select * from m where
    envios<>(select count_big(*) from {{ source('warehouse','fact_marketing') }})
    or sesiones<>(select count_big(*) from {{ source('warehouse','fact_web') }})
    or documentos<>(select count_big(distinct documento_nk) from {{ source('warehouse','fact_ventas') }})
    or neto<>(select sum(venta_neta_clp) from {{ ref('ventas_mensuales') }})
