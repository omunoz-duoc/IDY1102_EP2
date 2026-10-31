-- Transformación del mart dm_clientes; materialización candidata, sin modificar dm_*.
WITH documentos AS (
    SELECT c.cliente_nk,f.documento_nk,f.fecha_key,f.canal_key,
        SUM(CAST(f.cantidad AS bigint)) unidades,SUM(f.monto_neto) venta
    FROM {{ source('warehouse', 'fact_ventas') }} f JOIN {{ source('warehouse', 'dim_cliente') }} c ON f.cliente_key=c.cliente_key
    WHERE f.cliente_key<>0 AND f.fecha_key<={{ var('customer_cutoff_key') | int }}
    GROUP BY c.cliente_nk,f.documento_nk,f.fecha_key,f.canal_key),
    reembolsos AS (
    SELECT c.cliente_nk,f.fecha_key,CAST(f.cantidad AS bigint) unidades,f.monto_neto_devuelto importe
    FROM {{ source('warehouse', 'fact_devoluciones') }} f JOIN {{ source('warehouse', 'dim_cliente') }} c ON f.cliente_key=c.cliente_key
    WHERE f.cliente_key<>0 AND f.fecha_key<={{ var('customer_cutoff_key') | int }}),
    eventos AS (
    SELECT cliente_nk,fecha_key FROM documentos UNION ALL
    SELECT cliente_nk,fecha_key FROM reembolsos),
    inicios AS (
    SELECT cliente_nk,MIN(fecha_key) primera_actividad_key FROM eventos GROUP BY cliente_nk),
    limite AS (
    SELECT CAST({{ var('customer_cutoff_key') | int }} AS bigint) corte),
    periodos AS (
    SELECT d.anio,d.mes,MAX(d.fecha_key) fecha_corte_key,MAX(d.fecha) fecha_corte
    FROM {{ source('warehouse', 'dim_fecha') }} d CROSS JOIN limite l
    WHERE d.fecha_key>0 AND d.fecha_key<=l.corte
      AND d.fecha_key>=(SELECT MIN(primera_actividad_key) FROM inicios)
    GROUP BY d.anio,d.mes),
    compras_mes AS (
    SELECT v.cliente_nk,d.anio,d.mes,COUNT_BIG(*) compras,SUM(v.unidades) unidades,
        SUM(v.venta) venta,MIN(v.fecha_key) primera_compra,MAX(v.fecha_key) ultima_compra
    FROM documentos v JOIN {{ source('warehouse', 'dim_fecha') }} d ON v.fecha_key=d.fecha_key
    GROUP BY v.cliente_nk,d.anio,d.mes),
    devoluciones_mes AS (
    SELECT r.cliente_nk,d.anio,d.mes,COUNT_BIG(*) devoluciones,
        SUM(r.unidades) unidades,SUM(r.importe) importe
    FROM reembolsos r JOIN {{ source('warehouse', 'dim_fecha') }} d ON r.fecha_key=d.fecha_key
    GROUP BY r.cliente_nk,d.anio,d.mes),
    canales AS (
    SELECT v.cliente_nk,d.anio,d.mes,v.canal_key,COUNT_BIG(*) compras,SUM(v.venta) venta
    FROM documentos v JOIN {{ source('warehouse', 'dim_fecha') }} d ON v.fecha_key=d.fecha_key
    GROUP BY v.cliente_nk,d.anio,d.mes,v.canal_key),
    canal_ordenado AS (
    SELECT *,ROW_NUMBER() OVER(PARTITION BY cliente_nk,anio,mes
        ORDER BY compras DESC,venta DESC,canal_key ASC) orden FROM canales),
    base AS (
    SELECT i.cliente_nk,p.anio,p.mes,p.fecha_corte_key,p.fecha_corte,
        COALESCE(v.compras,0) compras,COALESCE(r.devoluciones,0) devoluciones,
        COALESCE(v.unidades,0) unidades_compradas,COALESCE(r.unidades,0) unidades_devueltas,
        COALESCE(v.venta,0) venta,COALESCE(r.importe,0) reembolso,
        COALESCE(v.venta,0)-COALESCE(r.importe,0) neto,
        v.primera_compra,v.ultima_compra,COALESCE(c.canal_key,0) canal_principal
    FROM inicios i JOIN periodos p ON p.fecha_corte_key>=i.primera_actividad_key
    LEFT JOIN compras_mes v ON i.cliente_nk=v.cliente_nk AND p.anio=v.anio AND p.mes=v.mes
    LEFT JOIN devoluciones_mes r ON i.cliente_nk=r.cliente_nk AND p.anio=r.anio AND p.mes=r.mes
    LEFT JOIN canal_ordenado c ON i.cliente_nk=c.cliente_nk AND p.anio=c.anio AND p.mes=c.mes AND c.orden=1),
    ventanas AS (
    SELECT *,
        SUM(compras) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS UNBOUNDED PRECEDING) compras_acumuladas,
        SUM(neto) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS UNBOUNDED PRECEDING) neto_acumulado,
        MAX(ultima_compra) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS UNBOUNDED PRECEDING) ultima_hasta_corte,
        SUM(compras) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) frecuencia_12m,
        SUM(neto) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) monetario_12m,
        COUNT_BIG(*) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) meses_observados
    FROM base),
resultado (cliente_nk, anio, mes, fecha_corte_key, cliente_key_corte, sin_version_al_corte, compras, devoluciones, unidades_compradas, unidades_devueltas, venta_sin_devoluciones_clp, devoluciones_clp, venta_neta_clp, primera_compra_mes_key, ultima_compra_mes_key, ultima_compra_hasta_corte_key, canal_principal_mes_key, compras_acumuladas, venta_neta_acumulada_clp, frecuencia_12m, monetario_12m_clp, meses_observados_12m, recencia_dias, activo_6m) AS (
SELECT v.cliente_nk,v.anio,v.mes,v.fecha_corte_key,COALESCE(c.cliente_key,0),
        CAST(CASE WHEN c.cliente_key IS NULL THEN 1 ELSE 0 END AS bit),
        v.compras,v.devoluciones,v.unidades_compradas,v.unidades_devueltas,
        v.venta,v.reembolso,v.neto,v.primera_compra,v.ultima_compra,v.ultima_hasta_corte,
        v.canal_principal,v.compras_acumuladas,v.neto_acumulado,v.frecuencia_12m,v.monetario_12m,v.meses_observados,
        DATEDIFF(day,u.fecha,v.fecha_corte),
        CAST(CASE WHEN u.fecha>=DATEADD(month,-6,v.fecha_corte) THEN 1 ELSE 0 END AS bit)
    FROM ventanas v LEFT JOIN {{ source('warehouse', 'dim_fecha') }} u ON v.ultima_hasta_corte=u.fecha_key
    LEFT JOIN {{ source('warehouse', 'dim_cliente') }} c ON c.cliente_nk=v.cliente_nk
        AND c.valido_desde<DATEADD(day,1,v.fecha_corte)
        AND c.valido_hasta>=DATEADD(day,1,v.fecha_corte)
)
SELECT * FROM resultado
