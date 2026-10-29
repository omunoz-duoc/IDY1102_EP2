select 'comercial' mart from {{ ref('ventas_mensuales') }}
where venta_neta_clp<>venta_sin_devoluciones_clp-devoluciones_clp
    or mes not between 1 and 12 or documentos<0 or venta_sin_devoluciones_clp<0 or devoluciones_clp<0
union all
select 'marketing' from {{ ref('campanias_resumen') }}
where venta_neta_asociada_clp<>venta_reconocida_clp-devoluciones_clp or conversiones_entregadas>entregados
union all
select 'clientes' from {{ ref('clientes_mensuales') }}
where venta_neta_clp<>venta_sin_devoluciones_clp-devoluciones_clp
    or compras<0 or devoluciones<0 or recencia_dias<0 or meses_observados_12m not between 1 and 12
