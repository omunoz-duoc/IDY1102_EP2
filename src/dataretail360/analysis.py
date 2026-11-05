"""Indicadores y visualizaciones calculados sobre los data marts publicados en SQL Server.

Los resultados no se inventan: tablas y gráficos se generan desde el destino persistente.
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.patches import FancyBboxPatch, Rectangle  # noqa: E402
from sqlalchemy import text  # noqa: E402

from .observability import pipeline_step  # noqa: E402


def query(engine, sql, params=None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


@dataclass
class Analysis:
    kpi: pd.DataFrame
    monthly: pd.DataFrame
    active: pd.DataFrame
    customer_period: pd.DataFrame
    cutoff: pd.Timestamp


def business_queries(ctx, cutoff: pd.Timestamp) -> dict:
    """Consumo desde los data marts publicados en SQL Server."""
    dw, report = ctx.dw, ctx.report
    report.section('6. Indicadores de negocio')
    kpi = query(dw, """SELECT
 SUM(venta_sin_devoluciones_clp) venta_antes_devoluciones,
 SUM(devoluciones_clp) devoluciones_netas,SUM(documentos) documentos,
 (SELECT COUNT_BIG(*) FROM dm_comercial.fact_ventas) lineas
 FROM dm_comercial.ventas_mensuales""")
    kpi['venta_neta'] = kpi.venta_antes_devoluciones - kpi.devoluciones_netas
    kpi['ticket_promedio'] = kpi.venta_antes_devoluciones / kpi.documentos
    report.table(kpi, 'KPI comerciales')
    monthly = query(dw, """SELECT m.anio,m.mes,c.canal,SUM(m.venta_neta_clp) venta_neta
 FROM dm_comercial.ventas_mensuales m JOIN dm_comercial.dim_canal c ON m.canal_key=c.canal_key
 GROUP BY m.anio,m.mes,c.canal ORDER BY m.anio,m.mes,c.canal""")
    territory = query(dw, """SELECT s.region,SUM(m.venta_neta_clp) venta_neta
 FROM dm_comercial.ventas_mensuales m JOIN dm_comercial.dim_sucursal s ON m.sucursal_key=s.sucursal_key
 GROUP BY s.region ORDER BY venta_neta DESC""")
    report.table(territory, 'Venta neta por región de sucursal')
    categories = query(dw, '''SELECT p.linea,p.categoria,SUM(f.monto_neto) venta_antes_devoluciones,
 SUM(f.cantidad) unidades FROM dm_comercial.fact_ventas f
 JOIN dm_comercial.dim_producto p ON f.producto_key=p.producto_key
 GROUP BY p.linea,p.categoria ORDER BY venta_antes_devoluciones DESC''')
    report.table(categories.head(15), 'Venta por línea y categoría')
    active = query(dw, '''SELECT COUNT_BIG(DISTINCT c.cliente_nk) clientes_activos
 FROM dm_clientes.fact_ventas f JOIN dm_clientes.dim_cliente c ON f.cliente_key=c.cliente_key
 WHERE f.fecha_key BETWEEN :desde AND :hasta AND f.cliente_key<>0''',
                   {'desde': int((cutoff - pd.DateOffset(months=6)).strftime('%Y%m%d')),
                    'hasta': int(cutoff.strftime('%Y%m%d'))})
    report.table(active, 'Clientes activos (6 meses al corte)')
    campaign_performance = query(dw, """SELECT codigo_campania,calidad_campania,envios,entregados,
 conversiones_entregadas,tasa_conversion_reportada,monto_atribuido,retorno_atribuido,
 sin_consentimiento,sesiones,venta_neta_asociada_clp
 FROM dm_marketing.v_kpis_campanias ORDER BY tasa_conversion_reportada DESC""")
    report.table(campaign_performance.head(10), 'Desempeño de campañas')
    inventory_status = query(dw, '''SELECT COUNT_BIG(*) posiciones,
 SUM(CAST(riesgo_reposicion AS bigint)) bajo_minimo,
 SUM(CAST(sin_conteo AS bigint)) sin_conteo,
 SUM(CAST(conteo_antiguo AS bigint)) conteo_antiguo
 FROM dm_operaciones.inventario_actual''')
    report.table(inventory_status, 'Estado del inventario al último corte')
    inventory_cut = query(dw, 'SELECT MAX(fecha_key) corte FROM dm_operaciones.inventario_actual').iloc[0, 0]
    stock_end = pd.to_datetime(str(inventory_cut), format='%Y%m%d')
    rotation = query(dw, '''WITH v AS (
 SELECT producto_key,sucursal_key,SUM(cantidad) unidades FROM dm_comercial.fact_ventas
 WHERE fecha_key BETWEEN :desde AND :hasta AND canal_key=1
 GROUP BY producto_key,sucursal_key), i AS (
 SELECT producto_key,sucursal_key,AVG(CAST(stock_sistema AS float)) stock_promedio,
 COUNT_BIG(*) cortes FROM dm_operaciones.fact_inventario WHERE fecha_key BETWEEN :desde AND :hasta
 GROUP BY producto_key,sucursal_key)
 SELECT TOP 15 p.producto,p.categoria,s.sucursal,i.stock_promedio,i.cortes,
 COALESCE(v.unidades,0) unidades_4_semanas,
 COALESCE(v.unidades,0)/NULLIF(i.stock_promedio,0) rotacion_4_semanas
 FROM i LEFT JOIN v ON i.producto_key=v.producto_key AND i.sucursal_key=v.sucursal_key
 JOIN dm_operaciones.dim_producto p ON i.producto_key=p.producto_key
 JOIN dm_operaciones.dim_sucursal s ON i.sucursal_key=s.sucursal_key
 WHERE i.stock_promedio>0 ORDER BY rotacion_4_semanas,i.stock_promedio DESC''',
                     {'desde': int((stock_end - pd.Timedelta(days=27)).strftime('%Y%m%d')), 'hasta': int(inventory_cut)})
    report.table(rotation, 'Rotación de 4 semanas (menor primero)')
    report.text('La rotación usa unidades vendidas POS antes de devoluciones y el promedio de **cortes disponibles** '
                'en cuatro semanas. No equivale a semanas de cobertura ni a una medición diaria exacta.')
    web_funnel = query(dw, """SELECT SUM(sesiones) sesiones,SUM(con_carrito) con_carrito,
 SUM(con_checkout) con_checkout,SUM(compra_reportada) compra_reportada
 FROM dm_marketing.campanias_resumen""")
    report.table(web_funnel, 'Embudo web')
    return {'kpi': kpi, 'monthly': monthly, 'active': active}


def customer_queries(ctx, cutoff: pd.Timestamp) -> pd.DataFrame:
    """Comportamiento al mismo corte de negocio que el indicador de cliente activo."""
    dw, report = ctx.dw, ctx.report
    corte = {'corte': int(cutoff.strftime('%Y%m%d'))}
    customer_period = query(dw, '''SELECT fecha_corte_key,COUNT_BIG(*) clientes_observados,
    SUM(CAST(activo_6m AS bigint)) activos_6m,
    SUM(CASE WHEN compras>0 THEN CAST(1 AS bigint) ELSE 0 END) compradores_mes,
    SUM(CASE WHEN compras_acumuladas>=2 THEN CAST(1 AS bigint) ELSE 0 END) recurrentes_observados,
    SUM(venta_neta_clp) venta_neta_identificada_mes_clp
    FROM dm_clientes.clientes_mensuales WHERE fecha_corte_key=:corte GROUP BY fecha_corte_key''', corte)
    customer_rfm = query(dw, '''SELECT fecha_corte_key,activo_6m,COUNT_BIG(*) clientes,
    AVG(CAST(recencia_dias AS float)) recencia_media_dias,
    AVG(CAST(frecuencia_12m AS float)) frecuencia_media_12m,
    SUM(monetario_12m_clp) monetario_12m_clp
    FROM dm_clientes.clientes_mensuales WHERE fecha_corte_key=:corte
    GROUP BY fecha_corte_key,activo_6m ORDER BY activo_6m DESC''', corte)
    customer_coverage = query(dw, '''SELECT identificado,dentro_cobertura_compras,SUM(compras) compras,
    SUM(venta_sin_devoluciones_clp) venta_sin_devoluciones_clp,
    SUM(devoluciones_clp) devoluciones_clp,SUM(venta_neta_clp) venta_neta_clp
    FROM dm_clientes.v_cobertura_identidad GROUP BY identificado,dentro_cobertura_compras
    ORDER BY dentro_cobertura_compras DESC,identificado DESC''')
    report.section('Consumo del data mart Clientes', level=3)
    report.table(customer_period, 'Población al corte')
    report.table(customer_rfm, 'RFM observado al corte')
    report.table(customer_coverage, 'Cobertura de identidad (identificada vs anónima)')
    report.text('''Los ejemplos usan el corte de negocio configurado en el caso para comparar el mismo indicador
de cliente activo. Las devoluciones posteriores permanecen en cobertura con bandera 0, sin crear periodos falsos
de inactividad. Las compras anónimas solo participan en cobertura financiera. Las medidas RFM son observadas,
con hasta 12 meses calendario disponibles; no son predicciones de abandono ni de valor de vida.''')
    return customer_period


def charts_and_lineage(ctx, state, monthly: pd.DataFrame) -> None:
    """Series y calidad con unidades y periodo explícitos; linaje de una medida sin datos personales."""
    plt.rcParams.update({'figure.dpi': 110, 'axes.spines.top': False, 'axes.spines.right': False})
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
    plot = monthly.assign(periodo=pd.to_datetime(dict(year=monthly.anio, month=monthly.mes, day=1)))
    for canal, group in plot.groupby('canal'):
        axes[0].plot(group.periodo, group.venta_neta / 1_000_000, label=canal)
    axes[0].set(title='Venta neta por canal y mes', ylabel='Millones de CLP', xlabel='Mes local de Chile')
    axes[0].legend(fontsize=8)
    axes[0].tick_params(axis='x', rotation=40)
    quality_df = state.auditor.quality_frame()
    problem = quality_df.loc[quality_df.rechazadas.gt(0)].nlargest(8, 'rechazadas')
    axes[1].barh(problem.etapa, problem.rechazadas, color='#bb5c3d')
    axes[1].set(title='Filas apartadas por etapa', xlabel='Filas (etapas no sumables entre sí)')
    axes[1].invert_yaxis()
    fig.tight_layout()
    chart = ctx.paths.gold / 'resumen.png'
    fig.savefig(chart, bbox_inches='tight')
    report_chart = ctx.report_path.parent / 'resumen.png'
    report_chart.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(report_chart, bbox_inches='tight')
    plt.close(fig)
    ctx.report.section('Gráficos', level=3)
    ctx.report.image(report_chart, ctx.report_path.parent, 'Venta neta por canal y filas apartadas')
    ctx.report.text('El periodo transaccional de referencia termina en el corte del dataset. Los meses posteriores '
                    'pueden contener solamente devoluciones de ventas anteriores; no deben compararse como meses '
                    'completos de venta. Las etapas tienen granos distintos y sus rechazos no se suman entre sí.')
    raw = state.bronze.raw
    sample = state.facts.tables['fact_ventas'].iloc[0]
    source_name = 'pos_ventas_detalle' if sample.fuente == 'ventas_POS' else 'ecom_pedidos_detalle'
    source_row = raw[source_name].loc[raw[source_name]._src_row.eq(sample._src_row)].iloc[0]
    assert source_row._batch == sample._batch
    lineage = pd.DataFrame([{'tabla_destino': 'dw.fact_ventas', 'medida': 'monto_neto',
                             'regla': 'Prorrateo entero del neto reconocido de cabecera', 'fuente': source_name,
                             'fila_bronze': int(sample._src_row), 'hash_bronze': sample._batch,
                             'fila_cabecera': int(sample.cabecera_row), 'hash_cabecera': sample.cabecera_batch}])
    ctx.report.table(lineage, 'Linaje de una medida')


def run_analysis(ctx, state) -> Analysis:
    cutoff = state.bronze.customer_cutoff
    with pipeline_step(ctx.log, 'analisis.consultas_negocio'):
        results = business_queries(ctx, cutoff)
    with pipeline_step(ctx.log, 'analisis.clientes'):
        customer_period = customer_queries(ctx, cutoff)
    with pipeline_step(ctx.log, 'analisis.graficos'):
        charts_and_lineage(ctx, state, results['monthly'])
    return Analysis(kpi=results['kpi'], monthly=results['monthly'], active=results['active'],
                    customer_period=customer_period, cutoff=cutoff)


def bus_matrix_figure() -> bytes:
    """Vista del modelo como matriz de bus: filas = hechos, columnas = dimensiones (PNG)."""
    dims = [('Fecha', 'fecha_key', 'día / mes / año'), ('Producto', 'producto_key', 'categoría · SCD2'),
            ('Sucursal', 'sucursal_key', 'geografía · SCD1/3'), ('Cliente', 'cliente_key', 'segmento · SCD2'),
            ('Canal', 'canal_key', 'tienda / web / app'), ('Campaña', 'campania_key', 'periodo / presupuesto')]
    facts = [('Ventas', 'Línea de documento', [0, 1, 2, 3, 4, 5]),
             ('Devoluciones', 'Registro de devolución', [0, 1, 2, 3, 4]),
             ('Inventario', 'Corte semanal × producto × sucursal', [0, 1, 2]),
             ('Marketing', 'Envío a cliente', [0, 3, 5]),
             ('Web', 'Sesión', [0, 1, 3, 5]),
             ('Reposición', 'Orden; solicitud y recepción', [0, 1, 2])]
    DIM_FILL, FACT_FILL, EDGE = '#e4eef9', '#e4f1ea', '#65758b'
    INK, SUB, MUTED, GRID, STRIPE, DOT = '#18314d', '#334155', '#45607c', '#dde3ea', '#f5f7fa', '#2f5d8a'
    W, H = 14, 8.6
    fact_x, fact_w = 0.3, 3.4
    col_x = [4.7 + j * 1.6 for j in range(len(dims))]
    col_w = 1.5
    head_y, head_h = 6.35, 1.0
    row_step, row_h = 0.85, 0.64
    row_y = [5.75 - i * row_step for i in range(len(facts))]
    x_end = col_x[-1] + col_w / 2 + 0.1
    fig, ax = plt.subplots(figsize=(W, H - 0.5))
    ax.set_xlim(0, W)
    ax.set_ylim(0.5, H)
    ax.axis('off')
    ax.text(0.3, 8.35, 'DataRetail 360 · Modelo dimensional', fontsize=20, weight='bold', color=INK, va='center')
    ax.text(0.3, 7.95, 'Matriz de bus: cada punto es una relación dimensión 1 → N hecho', fontsize=11, color=MUTED,
            va='center')
    for i, y in enumerate(row_y):
        if i % 2 == 0:
            ax.add_patch(Rectangle((fact_x - 0.1, y - row_step / 2), x_end - fact_x + 0.1, row_step,
                                   facecolor=STRIPE, edgecolor='none', zorder=0))
    for x in col_x:
        ax.plot([x, x], [row_y[-1] - row_step / 2, head_y], color=GRID, lw=1, zorder=1)
    for y in row_y:
        ax.plot([fact_x + fact_w, x_end], [y, y], color=GRID, lw=1, zorder=1)
    ax.text(col_x[0] - col_w / 2, head_y + head_h + 0.16, 'Dimensiones conformadas', fontsize=11, color=MUTED, va='center')
    for x, (name, key, note) in zip(col_x, dims):
        ax.add_patch(FancyBboxPatch((x - col_w / 2, head_y), col_w, head_h, boxstyle='round,pad=0.02,rounding_size=0.08',
                                    facecolor=DIM_FILL, edgecolor=EDGE, zorder=2))
        ax.text(x, head_y + 0.72, name, fontsize=12, weight='bold', color=INK, ha='center', va='center', zorder=3)
        ax.text(x, head_y + 0.45, key, fontsize=8, color=SUB, ha='center', va='center', family='monospace', zorder=3)
        ax.text(x, head_y + 0.2, note, fontsize=7.6, color=SUB, ha='center', va='center', zorder=3)
    ax.text(fact_x, head_y + 0.2, 'Hechos y granularidad', fontsize=11, color=MUTED, va='center')
    for y, (name, grain, links) in zip(row_y, facts):
        ax.add_patch(FancyBboxPatch((fact_x, y - row_h / 2), fact_w, row_h, boxstyle='round,pad=0.02,rounding_size=0.08',
                                    facecolor=FACT_FILL, edgecolor=EDGE, zorder=2))
        ax.text(fact_x + 0.15, y + 0.12, name, fontsize=12, weight='bold', color=INK, va='center', zorder=3)
        ax.text(fact_x + 0.15, y - 0.15, grain, fontsize=8.5, color=SUB, va='center', zorder=3)
        ax.text(fact_x + fact_w - 0.12, y + 0.12, f'{len(links)} dim', fontsize=8, color=MUTED, ha='right',
                va='center', zorder=3)
        for j, x in enumerate(col_x):
            if j in links:
                ax.scatter(x, y, s=170, color=DOT, edgecolor='white', linewidth=1.5, zorder=4)
            else:
                ax.scatter(x, y, s=30, facecolor='white', edgecolor='#c3cbd6', linewidth=1, zorder=4)
    ax.text(0.3, 0.75, 'Las claves y restricciones completas se definen en el DWH (dw.*). '
            'Geografía se conserva por rol en sucursal y cliente.', fontsize=10, color=MUTED, va='center')
    buffer = BytesIO()
    fig.savefig(buffer, format='png', bbox_inches='tight', dpi=140)
    plt.close(fig)
    return buffer.getvalue()
