-- Transformación del mart dm_marketing; materialización candidata, sin modificar dm_*.
WITH envios AS (
    SELECT campania_key,COUNT_BIG(*) envios,
        SUM(CAST(entregado AS bigint)) entregados,SUM(CAST(abierto AS bigint)) abiertos,
        SUM(CAST(click AS bigint)) clicks,
        SUM(CASE WHEN entregado=1 AND convertido=1 THEN CAST(1 AS bigint) ELSE 0 END) conversiones,
        SUM(CAST(sin_consentimiento AS bigint)) sin_consentimiento,SUM(monto_atribuido) monto
    FROM {{ source('warehouse', 'fact_marketing') }} GROUP BY campania_key), sesiones AS (
    SELECT campania_key,COUNT_BIG(*) sesiones,
        SUM(CAST(agrego_carrito AS bigint)) carrito,SUM(CAST(inicio_checkout AS bigint)) checkout,
        SUM(CAST(compro AS bigint)) compra FROM {{ source('warehouse', 'fact_web') }} GROUP BY campania_key), ventas AS (
    SELECT campania_key,COUNT_BIG(DISTINCT documento_nk) documentos,SUM(monto_neto) venta
    FROM {{ source('warehouse', 'fact_ventas') }} GROUP BY campania_key), documentos AS (
    SELECT documento_nk,campania_key FROM {{ ref('int_documentos_campania') }}),
    devoluciones AS (
    SELECT d.campania_key,SUM(f.monto_neto_devuelto) devolucion
    FROM {{ source('warehouse', 'fact_devoluciones') }} f JOIN documentos d ON f.documento_nk=d.documento_nk
    GROUP BY d.campania_key),
resultado (campania_key, envios, entregados, abiertos, clicks, conversiones_entregadas, sin_consentimiento, monto_atribuido, sesiones, con_carrito, con_checkout, compra_reportada, documentos_asociados, venta_reconocida_clp, devoluciones_clp, venta_neta_asociada_clp) AS (
SELECT c.campania_key,COALESCE(e.envios,0),COALESCE(e.entregados,0),COALESCE(e.abiertos,0),
        COALESCE(e.clicks,0),COALESCE(e.conversiones,0),COALESCE(e.sin_consentimiento,0),COALESCE(e.monto,0),
        COALESCE(s.sesiones,0),COALESCE(s.carrito,0),COALESCE(s.checkout,0),COALESCE(s.compra,0),
        COALESCE(v.documentos,0),COALESCE(v.venta,0),COALESCE(d.devolucion,0),
        COALESCE(v.venta,0)-COALESCE(d.devolucion,0)
    FROM {{ source('warehouse', 'dim_campania') }} c LEFT JOIN envios e ON c.campania_key=e.campania_key
    LEFT JOIN sesiones s ON c.campania_key=s.campania_key
    LEFT JOIN ventas v ON c.campania_key=v.campania_key
    LEFT JOIN devoluciones d ON c.campania_key=d.campania_key
)
SELECT * FROM resultado
