-- Transformación del mart dm_operaciones; materialización candidata, sin modificar dm_*.
WITH resultado (fecha_key, producto_key, sucursal_key, stock_sistema, stock_fisico, stock_minimo, punto_reposicion, unidades_en_transito, sin_conteo, conteo_antiguo, diferencia_stock, riesgo_reposicion) AS (
SELECT fecha_key,producto_key,sucursal_key,stock_sistema,stock_fisico,
        stock_minimo,punto_reposicion,unidades_en_transito,sin_conteo,conteo_antiguo,
        diferencia_stock,riesgo_reposicion
    FROM {{ source('warehouse', 'fact_inventario') }}
    WHERE fecha_key=(SELECT MAX(fecha_key) FROM {{ source('warehouse', 'fact_inventario') }})
)
SELECT * FROM resultado
