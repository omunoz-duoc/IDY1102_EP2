-- La navegación completa de ambas jerarquías conserva el grano y los importes.
with navegacion as (
    select count_big(*) filas,sum(f.monto_neto) venta
    from {{ source('warehouse','fact_ventas') }} f
    join {{ source('warehouse','dim_producto') }} p on f.producto_key=p.producto_key
    join {{ source('warehouse','dim_subcategoria') }} sc on p.subcategoria_key=sc.subcategoria_key
    join {{ source('warehouse','dim_categoria') }} c on sc.categoria_key=c.categoria_key
    join {{ source('warehouse','dim_linea') }} l on c.linea_key=l.linea_key
    join {{ source('warehouse','dim_sucursal') }} s on f.sucursal_key=s.sucursal_key
    join {{ source('warehouse','dim_comuna') }} co on s.comuna_key=co.comuna_key
    join {{ source('warehouse','dim_region') }} r on co.region_key=r.region_key
    join {{ source('warehouse','dim_pais') }} pa on r.pais_key=pa.pais_key
), esperado as (
    select count_big(*) filas,sum(monto_neto) venta from {{ source('warehouse','fact_ventas') }}
)
select n.* from navegacion n cross join esperado e
where n.filas<>e.filas or n.venta<>e.venta
