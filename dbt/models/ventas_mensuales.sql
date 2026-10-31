-- Transformación del mart dm_comercial; materialización candidata, sin modificar dm_*.
WITH movimientos AS (
    SELECT f.fecha_key,f.sucursal_key,f.canal_key,
        SUM(f.monto_neto) venta,CAST(0 AS bigint) devoluciones,
        SUM(CAST(f.cantidad AS bigint)) unidades,CAST(0 AS bigint) unidades_devueltas,
        COUNT_BIG(DISTINCT f.documento_nk) documentos
    FROM {{ source('warehouse', 'fact_ventas') }} f GROUP BY f.fecha_key,f.sucursal_key,f.canal_key
    UNION ALL
    SELECT f.fecha_key,f.sucursal_key,f.canal_key,0,SUM(f.monto_neto_devuelto),0,
        SUM(CAST(f.cantidad AS bigint)),0
    FROM {{ source('warehouse', 'fact_devoluciones') }} f GROUP BY f.fecha_key,f.sucursal_key,f.canal_key),
resultado (anio, mes, sucursal_key, canal_key, venta_sin_devoluciones_clp, devoluciones_clp, venta_neta_clp, unidades_vendidas, unidades_devueltas, documentos) AS (
SELECT d.anio,d.mes,m.sucursal_key,m.canal_key,SUM(m.venta),SUM(m.devoluciones),
        SUM(m.venta)-SUM(m.devoluciones),SUM(m.unidades),SUM(m.unidades_devueltas),SUM(m.documentos)
    FROM movimientos m JOIN {{ source('warehouse', 'dim_fecha') }} d ON m.fecha_key=d.fecha_key
    GROUP BY d.anio,d.mes,m.sucursal_key,m.canal_key
)
SELECT * FROM resultado
