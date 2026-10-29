-- Si devuelve una fila, se impide promover los candidatos a la publicación final.
with resumen as (
    select sum(venta_sin_devoluciones_clp) venta, sum(devoluciones_clp) devolucion,
        sum(venta_neta_clp) neto, sum(documentos) documentos
    from {{ ref('ventas_mensuales') }}
), esperado as (
    select (select sum(monto_neto) from {{ source('warehouse', 'fact_ventas') }}) venta,
        (select sum(monto_neto_devuelto) from {{ source('warehouse', 'fact_devoluciones') }}) devolucion,
        (select count_big(distinct documento_nk) from {{ source('warehouse', 'fact_ventas') }}) documentos
)
select r.* from resumen r cross join esperado e
where r.venta <> e.venta or r.devolucion <> e.devolucion
    or r.neto <> e.venta-e.devolucion or r.documentos <> e.documentos
