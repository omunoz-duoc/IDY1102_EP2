with esperado as (
    select count_big(*) filas, sum(stock_sistema) stock
    from {{ source('warehouse', 'fact_inventario') }}
    where fecha_key=(select max(fecha_key) from {{ source('warehouse', 'fact_inventario') }})
), actual as (
    select count_big(*) filas, sum(stock_sistema) stock from {{ ref('inventario_actual') }}
)
select a.* from actual a cross join esperado e where a.filas<>e.filas or a.stock<>e.stock
union all
select cast(fecha_key as bigint),cast(stock_sistema as bigint) from {{ ref('inventario_actual') }}
where fecha_key<>(select max(fecha_key) from {{ source('warehouse', 'fact_inventario') }}) or stock_sistema<0
