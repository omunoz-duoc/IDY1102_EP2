with actual as (
    select sum(compras) compras,sum(venta_sin_devoluciones_clp) ventas,
        sum(devoluciones_clp) devoluciones from {{ ref('clientes_mensuales') }}
), esperado as (
    select count_big(distinct documento_nk) compras,sum(monto_neto) ventas
    from {{ source('warehouse','fact_ventas') }} where cliente_key<>0 and fecha_key<={{ var('customer_cutoff_key') | int }}
)
select a.* from actual a cross join esperado e
where a.compras<>e.compras or a.ventas<>e.ventas
    or a.devoluciones<>(select sum(monto_neto_devuelto) from {{ source('warehouse','fact_devoluciones') }}
        where cliente_key<>0 and fecha_key<={{ var('customer_cutoff_key') | int }})
