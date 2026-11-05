"""Capa Gold en archivos y evidencia de la publicación.

El DWH es el destino autoritativo; Gold son exportaciones verificables de lo publicado.
La publicación SQL es atómica; las exportaciones y artefactos son posteriores y el manifiesto
refleja si terminaron. No se promete una transacción distribuida entre SQL Server y archivos.
"""
from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import text

from .observability import pipeline_step, report_data_warnings
from .warehouse.marts import MARTS


def export_warehouse(ctx, warehouse) -> None:
    gold = ctx.paths.gold
    with pipeline_step(ctx.log, 'gold.exportar_dwh'):
        for name, frame in warehouse.physical.items():
            with pipeline_step(ctx.log, 'gold.exportar_dimension', object=name, rows_total=len(frame)):
                frame.to_parquet(gold / f'{name}.parquet', index=False)
        for name, frame in warehouse.facts.items():
            with pipeline_step(ctx.log, 'gold.exportar_hecho', object=name, rows_total=len(frame)):
                frame.to_parquet(gold / f'{name}.parquet', index=False)
        # Ejemplo particionado de ventas: solo esta copia analítica se particiona por año/mes.
        partitioned = warehouse.facts['fact_ventas'].copy()
        partitioned['anio'] = partitioned.fecha_key // 10000
        partitioned['mes'] = (partitioned.fecha_key // 100) % 100
        partitioned.to_parquet(gold / f'ventas_particionadas_{ctx.run_id}', partition_cols=['anio', 'mes'], index=False)
        ctx.manifest.update({'status': 'SQL_PUBLISHED',
                             'published_tables': {n: len(f) for n, f in warehouse.all_frames.items()}})
        ctx.write_manifest()


def export_marts(ctx, warehouse) -> tuple[dict, pd.DataFrame]:
    """Cuatro productos por área, persistidos en SQL Server y exportados a Gold."""
    mart_frames, catalog = {}, []
    with pipeline_step(ctx.log, 'gold.exportar_marts'):
        for schema, spec in MARTS.items():
            with pipeline_step(ctx.log, 'gold.exportar_mart', object=schema + '.' + spec['tabla']) as step:
                qualified = schema + '.' + spec['tabla']
                with ctx.dw.connect() as conn:
                    frame = pd.read_sql(text(f'SELECT * FROM {qualified}'), conn)
                folder = ctx.paths.gold / 'datamarts' / schema
                folder.mkdir(parents=True, exist_ok=True)
                frame.to_parquet(folder / f'{spec["tabla"]}.parquet', index=False)
                mart_frames[qualified] = frame
                catalog.append({'datamart': schema, 'area': spec['nombre'], 'tabla_persistida': qualified,
                                'grano': spec['grano'], 'filas': len(frame),
                                'hechos_por_vistas': ', '.join(spec['hechos']),
                                'dimensiones_compartidas': ', '.join(spec['dimensiones']), 'preguntas': spec['uso']})
                step.progress(rows=len(frame))
        catalog = pd.DataFrame(catalog)
        catalog.to_parquet(ctx.paths.gold / 'datamarts' / 'catalogo.parquet', index=False)
        ctx.manifest['datamarts'] = catalog.to_dict('records')
        ctx.manifest['customer_coverage_cutoff_key'] = warehouse.cutoff_key
        ctx.manifest['status'] = 'OK'
        ctx.write_manifest()
        with ctx.dw.connect() as conn:
            published = pd.read_sql(text('''SELECT datamart,tabla,filas,publicado_utc
            FROM ctl.datamart_publicaciones WHERE run_id=:id ORDER BY datamart'''), conn, params={'id': ctx.run_id})
        ctx.report.section('5. Publicación del DWH y data marts')
        ctx.report.table(catalog[['datamart', 'tabla_persistida', 'grano', 'filas']], 'Catálogo de data marts publicados')
        ctx.report.table(published, 'Registro ctl.datamart_publicaciones de esta corrida')
        report_data_warnings(ctx.log, mart_frames)
    return mart_frames, catalog


def dbt_evidence(ctx, warehouse, mart_frames) -> dict:
    """Resultados, dependencias y reglas legibles del build dbt aprobado."""
    with pipeline_step(ctx.log, 'dbt.presentar_evidencia', heartbeat=False):
        context = warehouse.runs[ctx.run_id]
        folder = ctx.paths.home / context['artifact_path']
        graph = json.loads((folder / 'build_manifest.json').read_text(encoding='utf-8'))
        results = json.loads((folder / 'build_run_results.json').read_text(encoding='utf-8'))
        status_by_id = {r['unique_id']: r for r in results['results']}
        lineage = []
        for key, node in graph['nodes'].items():
            if node['resource_type'] != 'model':
                continue
            schema = next((s for s, v in MARTS.items() if v['tabla'] == node['name']), None)
            lineage.append({'modelo': node['name'], 'materialización': node['config']['materialized'],
                            'dependencias': ', '.join(d.split('.')[-1] for d in node['depends_on']['nodes']),
                            'estado': status_by_id[key]['status'],
                            'segundos': round(status_by_id[key]['execution_time'], 2),
                            'filas_publicadas': len(mart_frames[schema + '.' + node['name']]) if schema else None})
        test_rows = [{'prueba': row['unique_id'].split('.', 2)[2].rsplit('.', 1)[0], 'resultado': row['status'],
                      'segundos': round(row['execution_time'], 2)}
                     for row in results['results'] if row['unique_id'].startswith('test.')]
        report = ctx.report
        report.section('Evidencia de dbt y promoción de los marts', level=3)
        report.table(pd.DataFrame(lineage))
        report.text(f"**{len(test_rows)} pruebas dbt aprobadas**; {context['models_built']} modelos construidos "
                    f"(4 tablas y 1 vista intermedia). Corte de Clientes: **{warehouse.cutoff_key}**. "
                    f"Promoción: **{context['publication_status']}**. Candidatos eliminados: **{context['candidates_cleaned']}**.")
        report.details('Pruebas dbt', _md(pd.DataFrame(test_rows)))
        report.text(f"Catálogo, grafo y documentación HTML en `{context['artifact_path']}` "
                    "(`static_index.html`). `build_manifest.json` y `build_run_results.json` conservan la evidencia "
                    "del build aprobado; `publication.json` registra versiones, hashes, corte y promoción.")
        for path in sorted((ctx.paths.dbt_project / 'models').glob('*.sql')):
            report.details(f'Modelo dbt: {path.stem}', f'```sql\n{path.read_text(encoding="utf-8")}\n```')
        ctx.manifest['dbt'] = context.copy()
        ctx.write_manifest()
    return {'graph': graph, 'results': results, 'test_rows': test_rows, 'folder': folder, 'context': context}


def _md(frame):
    from .report import markdown_table
    return markdown_table(frame, max_rows=None)
