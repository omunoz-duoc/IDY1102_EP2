"""Medición de optimización y conclusiones calculadas a partir de la ejecución verificada."""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from .analysis import query
from .observability import event, pipeline_step


def benchmark(ctx, state) -> None:
    """Formatos y selección de columnas; los tiempos no prometen aceleración."""
    gold, report = ctx.paths.gold, ctx.report
    with pipeline_step(ctx.log, 'pruebas.benchmark'):
        measure = state.facts.tables['fact_ventas'][['fecha_key', 'producto_key', 'cantidad', 'monto_neto']]
        csv_path, pq_path = gold / 'comparacion_ventas.csv', gold / 'comparacion_ventas.parquet'
        measure.to_csv(csv_path, index=False)
        measure.to_parquet(pq_path, index=False)
        times = []
        for fmt, path, read in [('CSV', csv_path, lambda: pd.read_csv(csv_path, usecols=['monto_neto'])),
                                ('Parquet', pq_path, lambda: pd.read_parquet(pq_path, columns=['monto_neto']))]:
            durations = []
            for _ in range(3):
                start = time.perf_counter()
                loaded = read()
                durations.append(time.perf_counter() - start)
            assert int(loaded.monto_neto.sum()) == int(measure.monto_neto.sum())
            times.append({'formato': fmt, 'bytes': path.stat().st_size, 'lectura_mediana_seg': float(np.median(durations))})
        report.section('9. Optimización medida')
        report.table(pd.DataFrame(times), 'Lectura CSV vs Parquet (columna única)')
        # Plan de ejecución: la inspección no ejecuta la consulta ni altera índices.
        odbc = ctx.dw.raw_connection()
        try:
            cursor = odbc.cursor()
            cursor.execute('SET SHOWPLAN_XML ON')
            cursor.execute('SELECT SUM(monto_neto) FROM dw.fact_ventas WHERE fecha_key BETWEEN 20260901 AND 20260930')
            plan_xml = cursor.fetchone()[0]
            cursor.execute('SET SHOWPLAN_XML OFF')
            cursor.close()
        finally:
            odbc.close()
        (ctx.paths.control / 'plan_consulta.xml').write_text(plan_xml, encoding='utf-8')
        report.text(f"Plan SQL Server capturado en `data/control/plan_consulta.xml`. Usa operador Index Seek: "
                    f"**{'Index Seek' in plan_xml}**.")
        # El benchmark compara venta antes de devoluciones: excluir grupos sin documentos de venta.
        timings, reference = [], None
        queries = {
            'Hecho detallado': '''SELECT d.anio,d.mes,f.canal_key,SUM(f.monto_neto) importe
 FROM dw.fact_ventas f JOIN dw.dim_fecha d ON f.fecha_key=d.fecha_key GROUP BY d.anio,d.mes,f.canal_key''',
            'Data mart Comercial': '''SELECT anio,mes,canal_key,SUM(venta_sin_devoluciones_clp) importe
 FROM dm_comercial.ventas_mensuales WHERE documentos>0 GROUP BY anio,mes,canal_key'''}
        for label, sql in queries.items():
            durations = []
            for _ in range(3):
                start = time.perf_counter()
                result = query(ctx.dw, sql)
                durations.append(time.perf_counter() - start)
            result = result.sort_values(['anio', 'mes', 'canal_key']).reset_index(drop=True)
            if reference is None:
                reference = result
            else:
                pd.testing.assert_frame_equal(reference, result, check_dtype=False)
            timings.append({'consulta': label, 'mediana_seg': float(np.median(durations)), 'filas': len(result)})
        report.table(pd.DataFrame(timings), 'Hecho detallado vs data mart (mismo resultado)')
        report.text('Tiempos locales con tres lecturas y posible caché. El particionamiento implementado es de '
                    '**archivos Parquet**; no se afirma particionar tablas de SQL Server. Parquet por sí solo no '
                    'aporta transacciones ACID ni time travel.')


def conclusions(ctx, state, analysis, checks) -> None:
    """Evidencia y conclusiones calculadas; cierra el manifiesto de la corrida."""
    report = ctx.report
    with pipeline_step(ctx.log, 'pipeline.cierre', heartbeat=False):
        sales_fact = state.facts.tables['fact_ventas']
        unknowns = pd.DataFrame([{'dimension': c, 'lineas_sin_resolver': int(sales_fact[c].eq(0).sum()),
                                  'porcentaje': round(100 * sales_fact[c].eq(0).mean(), 2)}
                                 for c in ['producto_key', 'sucursal_key', 'cliente_key', 'campania_key']])
        report.section('10. Conclusiones de la ejecución')
        report.table(unknowns, 'Miembros desconocidos en ventas')
        report.text('Campaña 0 también significa ausencia de campaña; no todo miembro 0 constituye un error. '
                    'Cliente 0 puede representar compra anónima.')
        report.table(state.bronze.controls['definiciones_kpi_por_area'], 'Definiciones de KPI por área (origen)')
        legacy = state.silver.legacy
        legacy_summary = (legacy.assign(anio=legacy.fecha_limpia.dt.year).groupby('anio')
                          .agg(filas=('fecha_limpia', 'size'), importe_reportado=('monto_limpio', 'sum')).reset_index())
        report.table(legacy_summary, 'Reportes manuales: cobertura independiente')
        kpi, dbt = analysis.kpi, state.dbt.get('context', {})
        inventory = state.bronze.inventory
        report.text(f'''- Se extrajeron **{int(inventory.filas.sum()):,}** registros de 16 conjuntos: 14 por PostgreSQL,
  campañas desde CSV y reportes manuales desde Excel. La instantánea SQL es de solo lectura;
  los archivos de entrada se conservaron por contenido con SHA256.
- Se publicaron **{len(sales_fact):,} líneas de venta** y cinco hechos complementarios en SQL Server.
- El DWH utiliza **{len(state.dims.physical)} tablas dimensionales**: seis dimensiones directas y seis niveles de copo de nieve.
- El pipeline culminó en **4 data marts dependientes**: Comercial, Operaciones, Marketing-Digital y Clientes.
- La venta reconocida antes de devoluciones fue **{int(kpi.venta_antes_devoluciones.iloc[0]):,} CLP**;
  las devoluciones netas aceptadas fueron **{int(kpi.devoluciones_netas.iloc[0]):,} CLP**.
  La venta neta resultante es **{int(kpi.venta_neta.iloc[0]):,} CLP**, bajo el contrato documentado.
- dbt construyó **{dbt.get("models_built", 0)} modelos** y aprobó **{dbt.get("tests_passed", 0)} pruebas de datos** antes de promover.
- Se verificaron **{len(checks)} controles** del pipeline.

**Límites.** La historia SCD comienza con observaciones del pipeline: la fuente no entrega cambios históricos.
Los estados actuales de pedidos no permiten reconstruir toda su evolución; se excluyen DEVUELTO del
reconocimiento para evitar doble descuento. El log no imprime datos personales, pero Bronze/Silver requieren
permisos de carpeta y retención.''')
        ctx.manifest.setdefault('observability', {}).update({'status': 'COMPLETED', 'checks_passed': len(checks)})
        ctx.write_manifest()
        event(ctx.log, 'run_completed', 'Pipeline completo: exportaciones y controles finalizados.',
              checks_passed=len(checks), tests_passed=dbt.get('tests_passed', 0), artifact=f'logs/{ctx.run_id}.jsonl')
