with calculado as (
    select *,sum(compras) over(partition by cliente_nk order by anio,mes rows between 11 preceding and current row) f,
        sum(venta_neta_clp) over(partition by cliente_nk order by anio,mes rows between 11 preceding and current row) m,
        max(ultima_compra_mes_key) over(partition by cliente_nk order by anio,mes rows unbounded preceding) u,
        sum(compras) over(partition by cliente_nk order by anio,mes rows unbounded preceding) acumuladas,
        sum(venta_neta_clp) over(partition by cliente_nk order by anio,mes rows unbounded preceding) neto_acumulado
    from {{ ref('clientes_mensuales') }}
)
select c.cliente_nk,c.anio,c.mes from calculado c
join {{ source('warehouse','dim_fecha') }} d on c.fecha_corte_key=d.fecha_key
left join {{ source('warehouse','dim_fecha') }} u on c.ultima_compra_hasta_corte_key=u.fecha_key
where frecuencia_12m<>f or monetario_12m_clp<>m or coalesce(c.ultima_compra_hasta_corte_key,0)<>coalesce(c.u,0)
    or coalesce(recencia_dias,-1)<>coalesce(datediff(day,u.fecha,d.fecha),-1)
    or compras_acumuladas<>acumuladas or venta_neta_acumulada_clp<>neto_acumulado
    or activo_6m<>cast(case when u.fecha>=dateadd(month,-6,d.fecha) then 1 else 0 end as bit)
