"""Pruebas de datos y errores sobre el destino real: se comprueban propiedades, no solo que el código termine.

Incluye una segunda publicación completa (idempotencia), rollbacks controlados, el caso ficticio
del mart Clientes, una prueba adversa de dbt y la integridad física del snowflake.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from .analysis import query
from .facts import allocate_integer
from .normalize import legacy_date
from .observability import event, pipeline_step
from .scd import validate_scd
from .snowflake import (DIMENSION_LINKS, HIERARCHIES, REMOVED_ATTRIBUTES, expand_dimension, normalize_dimensions,
                        validate_links)
from .warehouse.marts import MART_GRAIN, MARTS, check_customer_fixture, clear_datamarts, validate_customer_mart


def snowflake_selftest() -> int:
    """Seis casos controlados: homónimos, round-trip, reclasificación, reejecución, huérfanos y duplicados."""
    logical = {
        'dim_producto': pd.DataFrame([
            {'producto_key': 0, 'producto_nk': 'DESCONOCIDO', 'linea': 'DESCONOCIDO', 'categoria': 'DESCONOCIDO',
             'subcategoria': 'DESCONOCIDO'},
            {'producto_key': 1, 'producto_nk': 'A', 'linea': 'L1', 'categoria': 'C', 'subcategoria': 'S'},
            {'producto_key': 2, 'producto_nk': 'B', 'linea': 'L2', 'categoria': 'C', 'subcategoria': 'S'}]),
        'dim_sucursal': pd.DataFrame([
            {'sucursal_key': 0, 'pais': 'DESCONOCIDO', 'region': 'DESCONOCIDO', 'comuna': 'DESCONOCIDO',
             'zona': 'DESCONOCIDO'},
            {'sucursal_key': 1, 'pais': 'CHILE', 'region': 'R1', 'comuna': 'CENTRO', 'zona': 'NORTE'},
            {'sucursal_key': 2, 'pais': 'CHILE', 'region': 'R2', 'comuna': 'CENTRO', 'zona': 'SUR'}])}
    physical = normalize_dimensions(logical)
    for name in logical:  # 1. round-trip sin fan-out
        restored = expand_dimension(name, physical[name], physical)
        pd.testing.assert_frame_equal(logical[name], restored[logical[name].columns], check_dtype=False)
    # 2. homónimos con padres distintos
    assert len(physical['dim_categoria'].query("categoria=='C'")) == 2
    assert len(physical['dim_comuna'].query("comuna=='CENTRO'")) == 2
    # 3. reclasificación conserva rama y claves previas
    changed = {k: v.copy() for k, v in logical.items()}
    old = changed['dim_producto'].iloc[[1]].assign(producto_key=3, linea='L3')
    changed['dim_producto'] = pd.concat([changed['dim_producto'], old], ignore_index=True)
    second = normalize_dimensions(changed, physical)
    for name, (key, _, _) in HIERARCHIES.items():
        pd.testing.assert_frame_equal(physical[name],
                                      second[name].loc[second[name][key].isin(physical[name][key])].reset_index(drop=True))
    p = second['dim_producto'].set_index('producto_key')
    assert p.loc[1, 'subcategoria_key'] != p.loc[3, 'subcategoria_key']
    # 4. reejecución estable y miembro 0
    repeat = normalize_dimensions(logical, physical)
    for name in physical:
        pd.testing.assert_frame_equal(physical[name], repeat[name])
    for name, (key, _, label) in HIERARCHIES.items():
        assert physical[name].set_index(key).loc[0, label] == 'DESCONOCIDO'
    # 5. huérfano rechazado
    orphan = {k: v.copy() for k, v in physical.items()}
    orphan['dim_producto'].loc[0, 'subcategoria_key'] = 99999
    try:
        validate_links(orphan)
    except ValueError:
        pass
    else:
        raise AssertionError('Se aceptó una FK jerárquica huérfana')
    # 6. diccionario previo duplicado rechazado
    duplicated = {k: v.copy() for k, v in physical.items()}
    duplicated['dim_categoria'] = pd.concat([duplicated['dim_categoria'], duplicated['dim_categoria'].iloc[[0]]],
                                            ignore_index=True)
    try:
        normalize_dimensions(logical, duplicated)
    except ValueError:
        pass
    else:
        raise AssertionError('Se aceptó un diccionario jerárquico duplicado')
    return 6


class Verifier:
    def __init__(self, ctx, state, analysis):
        self.ctx, self.state, self.analysis = ctx, state, analysis
        self.log = ctx.log
        self.dw = ctx.dw
        self.wh = state.warehouse
        self.checks = list(state.bronze.checks)

    def q(self, sql, params=None):
        return query(self.dw, sql, params)

    def passed(self, name):
        self.checks.append({'prueba': name, 'resultado': 'OK'})
        event(self.log, 'check_passed', 'Control aprobado.', object=name, checks_passed=len(self.checks))

    def destination_signature(self):
        """Conteo y checksum por tabla final: control complementario, no prueba criptográfica."""
        result = {}
        names = ['dw.' + name for name in self.wh.all_frames] + [s + '.' + v['tabla'] for s, v in MARTS.items()]
        for name in names:
            result[name] = self.q(f'SELECT COUNT_BIG(*) n,CHECKSUM_AGG(BINARY_CHECKSUM(*)) checksum FROM {name}').iloc[0].to_dict()
        return result

    # -- cell 43 -----------------------------------------------------------------------------
    def integrity_and_second_publication(self):
        st, an, facts = self.state, self.analysis, self.state.facts.tables
        sales, kpi = st.facts.sales, an.kpi
        cutoff_key = st.cutoff_key
        assert legacy_date(datetime(2023, 1, 13)) == pd.Timestamp('2023-01-13')
        assert legacy_date('2023-01-13 00:00:00') == pd.Timestamp('2023-01-13')
        assert legacy_date('44999') == pd.Timestamp('1899-12-30') + pd.Timedelta(days=44999)
        assert pd.isna(legacy_date('01/02/2023'))
        assert pd.isna(legacy_date('2023-99-01 00:00:00'))
        self.checks.append({'prueba': 'Excel: fecha nativa, ISO y serial válidos; texto ambiguo en cuarentena',
                            'resultado': 'OK'})
        self.wh.validate()
        self.passed('Grano, PK, FK y medidas no negativas')
        assert int(kpi.venta_antes_devoluciones.iloc[0]) == int(sales.monto_neto.sum())
        self.passed('Cuadratura monetaria Pandas vs SQL Server')
        assert int(kpi.venta_neta.iloc[0]) == int(an.monthly.venta_neta.sum())
        self.passed('Venta neta vs agregación mensual')
        quality_df = st.auditor.quality_frame()
        assert quality_df.leidas.eq(quality_df.aceptadas + quality_df.rechazadas + quality_df.duplicadas).all()
        self.passed('Balance de filas por etapa')
        mini = pd.DataFrame({'doc': ['A'] * 3, 'peso': [1, 1, 1], 'neto': [10] * 3})
        assert list(allocate_integer(mini, 'doc', 'peso', 'neto')) == [4, 3, 3]
        self.passed('Prorrateo entero con resto')
        broken = {n: f.head(1).copy() for n, f in facts.items()}
        broken['fact_ventas'].loc[:, 'fecha_key'] = 99999999
        try:
            self.wh.validate(broken)
        except ValueError:
            self.passed('Quality gate detiene FK inválida antes de publicar')
        else:
            raise AssertionError('Se aceptó una clave inválida')
        probe = 'probe_' + uuid.uuid4().hex[:20]
        try:
            with self.dw.begin() as conn:
                conn.execute(text("INSERT INTO ctl.ejecuciones(run_id,estado) VALUES(:id,'TEST')"), {'id': probe})
                raise RuntimeError('Falla controlada para verificar rollback')
        except RuntimeError:
            pass
        assert self.q('SELECT COUNT(*) n FROM ctl.ejecuciones WHERE run_id=:id', {'id': probe}).iloc[0, 0] == 0
        self.passed('Rollback real en SQL Server ante una excepción')

        # Segunda publicación completa: conteos, importes y claves permanecen iguales.
        before = self.destination_signature()
        self.wh.publish(self.repeat_id)
        after = self.destination_signature()
        assert before == after
        self.passed('Segunda publicación real: mismos hechos y cuatro data marts, sin duplicados')
        silver = st.silver
        incoming = {'dim_cliente': silver.identities.client_master, 'dim_producto': silver.products.product_master,
                    'dim_sucursal': st.dims.store_master}
        nks = {'dim_cliente': ('cliente_nk', 'cliente_key'), 'dim_producto': ('producto_nk', 'producto_key'),
               'dim_sucursal': ('sucursal_nk', 'sucursal_key')}
        for name, inc in incoming.items():
            repeated = st.dims.merge_policy(name, inc)
            assert len(repeated) == len(st.dims.logical[name])
            validate_scd(repeated, *nks[name])
        self.passed('Reejecución de SCD sin crear nuevas versiones')

        # Pruebas de los data marts: integridad, conciliación y publicación atómica.
        mart_frames = st.mart_frames
        assert len(MARTS) == 4 and len(st.mart_catalog) == 4
        self.passed('Cuatro data marts físicos publicados en esquemas de SQL Server')
        for name, keys in MART_GRAIN.items():
            assert not mart_frames[name].duplicated(keys).any()
            assert not mart_frames[name][keys].isna().any().any()
        self.passed('Grano y claves únicas en las cuatro tablas de data mart')
        assert int(self.q('SELECT SUM(documentos) n FROM dm_comercial.ventas_mensuales').iloc[0, 0]) == sales.documento_nk.nunique()
        assert int(self.q('SELECT SUM(venta_neta_clp) n FROM dm_comercial.ventas_mensuales').iloc[0, 0]) == int(kpi.venta_neta.iloc[0])
        self.passed('Comercial concilia documentos distintos y venta neta con el DWH')
        ops = mart_frames['dm_operaciones.inventario_actual']
        inventory = facts['fact_inventario']
        latest_stock = inventory.loc[inventory.fecha_key.eq(inventory.fecha_key.max())]
        assert len(ops) == len(latest_stock) and ops.fecha_key.nunique() == 1
        assert int(ops.stock_sistema.sum()) == int(latest_stock.stock_sistema.sum())
        assert int(ops.riesgo_reposicion.sum()) == int(latest_stock.riesgo_reposicion.sum())
        self.passed('Operaciones conserva únicamente el último corte común y sus existencias')
        mk = mart_frames['dm_marketing.campanias_resumen']
        assert int(mk.envios.sum()) == len(facts['fact_marketing'])
        assert int(mk.sesiones.sum()) == len(facts['fact_web'])
        assert int(mk.documentos_asociados.sum()) == sales.documento_nk.nunique()
        assert int(mk.venta_neta_asociada_clp.sum()) == int(kpi.venta_neta.iloc[0])
        assert mk.conversiones_entregadas.le(mk.entregados).all()
        self.passed('Marketing integra agregados sin multiplicar envíos, sesiones o ingresos')
        objects = self.q('''SELECT s.name esquema,o.name objeto,RTRIM(o.type) tipo
        FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id
        WHERE s.name IN ('dm_comercial','dm_operaciones','dm_marketing','dm_clientes') AND o.type IN ('U','V')''')
        assert objects.query("tipo == 'U'").groupby('esquema').size().to_dict() == {s: 1 for s in MARTS}
        assert self.q('''SELECT COUNT_BIG(*) n FROM sys.foreign_keys
        WHERE parent_object_id IN (OBJECT_ID('dm_comercial.ventas_mensuales'),
        OBJECT_ID('dm_operaciones.inventario_actual'),OBJECT_ID('dm_marketing.campanias_resumen'),
        OBJECT_ID('dm_clientes.clientes_mensuales'))''').iloc[0, 0] == 12
        self.passed('Tablas persistentes, vistas y doce FK a dimensiones conformadas verificadas')
        before_mart_rollback = self.destination_signature()
        try:
            with self.dw.begin() as conn:
                clear_datamarts(conn)
                raise RuntimeError('Fallo controlado de refresco de data marts')
        except RuntimeError as exc:
            if str(exc) != 'Fallo controlado de refresco de data marts':
                raise
        assert self.destination_signature() == before_mart_rollback
        self.passed('Una falla controlada restaura los cuatro data marts mediante rollback')
        # Clientes: conciliación de la población identificada, cobertura y snapshots completos.
        cm = mart_frames['dm_clientes.clientes_mensuales']
        assert cm.cliente_nk.ne('DESCONOCIDO').all()
        assert not cm.duplicated(['cliente_nk', 'anio', 'mes']).any()
        with self.dw.connect() as conn:
            validate_customer_mart(conn, cutoff_key)
        cut = {'cut': cutoff_key}
        assert int(cm.compras.sum()) == int(self.q('SELECT COUNT_BIG(DISTINCT documento_nk) n FROM dw.fact_ventas '
                                                   'WHERE cliente_key<>0 AND fecha_key<=:cut', cut).iloc[0, 0])
        assert int(cm.venta_sin_devoluciones_clp.sum()) == int(self.q('SELECT SUM(monto_neto) n FROM dw.fact_ventas '
                                                                      'WHERE cliente_key<>0 AND fecha_key<=:cut', cut).iloc[0, 0])
        self.passed('Clientes: una identidad por mes, documentos distintos y ventas identificadas conciliadas')
        coverage = self.q('''SELECT SUM(venta_sin_devoluciones_clp) ventas,SUM(devoluciones_clp) devoluciones,
        SUM(venta_neta_clp) neto FROM dm_clientes.v_cobertura_identidad''').iloc[0]
        assert int(coverage.ventas) == int(kpi.venta_antes_devoluciones.iloc[0])
        assert int(coverage.devoluciones) == int(kpi.devoluciones_netas.iloc[0])
        assert int(coverage.neto) == int(kpi.venta_neta.iloc[0])
        covered = self.q('SELECT SUM(venta_neta_clp) n FROM dm_clientes.v_cobertura_identidad '
                         'WHERE identificado=1 AND dentro_cobertura_compras=1').iloc[0, 0]
        assert int(covered) == int(cm.venta_neta_clp.sum())
        self.passed('Clientes: cobertura identificada y anónima concilia con la venta total sin inventar personas')
        continuity = self.q('''SELECT COUNT_BIG(*) n FROM (
        SELECT cliente_nk,COUNT_BIG(*) observados,
            DATEDIFF(month,MIN(DATEFROMPARTS(anio,mes,1)),MAX(DATEFROMPARTS(anio,mes,1)))+1 esperados
        FROM dm_clientes.clientes_mensuales GROUP BY cliente_nk) x WHERE observados<>esperados''')
        assert int(continuity.iloc[0, 0]) == 0
        assert cm.compras.eq(0).any()
        assert int(cm.fecha_corte_key.max()) == cutoff_key
        assert len(cm.loc[cm.fecha_corte_key.eq(cutoff_key)]) == cm.cliente_nk.nunique()
        self.passed('Clientes: meses sin compra incluidos y ventanas calendario sin huecos')
        bad_windows = self.q('''WITH calculado AS (
        SELECT *,SUM(compras) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) f,
        SUM(venta_neta_clp) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) m,
        MAX(ultima_compra_mes_key) OVER(PARTITION BY cliente_nk ORDER BY anio,mes ROWS UNBOUNDED PRECEDING) u
        FROM dm_clientes.clientes_mensuales)
        SELECT COUNT_BIG(*) n FROM calculado c LEFT JOIN dw.dim_fecha d ON c.fecha_corte_key=d.fecha_key
        LEFT JOIN dw.dim_fecha u ON c.ultima_compra_hasta_corte_key=u.fecha_key
        WHERE frecuencia_12m<>f OR monetario_12m_clp<>m OR COALESCE(c.ultima_compra_hasta_corte_key,0)<>COALESCE(c.u,0)
           OR COALESCE(recencia_dias,-1)<>COALESCE(DATEDIFF(day,u.fecha,d.fecha),-1)''')
        assert int(bad_windows.iloc[0, 0]) == 0
        assert cm.recencia_dias.dropna().ge(0).all()
        self.passed('Clientes: frecuencia, monetario y recencia verificados al corte explícito')
        assert len(an.customer_period) == 1
        assert int(an.customer_period.activos_6m.iloc[0]) == int(an.active.clientes_activos.iloc[0])
        self.passed('Clientes: indicador activo de seis meses coincide para la misma fecha de negocio')
        with self.dw.begin() as conn:
            fixture = check_customer_fixture(conn, self.wh.dbt.refresh_sql['dm_clientes'])
        self.ctx.report.table(fixture, 'Caso controlado de Clientes: dos versiones SCD, un mes sin compra y devolución posterior')
        self.passed('Clientes: SQL de Clientes validado con dos SCD, meses vacíos y reembolsos dentro/fuera de cobertura')
        self.ctx.manifest['validation'] = {'checks_passed': len(self.checks), 'repeat_run_id': self.repeat_id,
                                           'datamarts_checked': len(MARTS)}
        self.ctx.write_manifest()

    @property
    def repeat_id(self):
        return self.ctx.run_id[:24] + '_repeat'

    # -- cell 44 -----------------------------------------------------------------------------
    def dbt_and_failure_probe(self):
        dbt = self.state.dbt
        context, test_rows, graph, folder = dbt['context'], dbt['test_rows'], dbt['graph'], dbt['folder']
        assert context['publication_status'] == 'OK' and context['tests_passed'] == len(test_rows)
        assert all(row['resultado'] == 'pass' for row in test_rows)
        assert context['candidates_cleaned']
        assert all(node['schema'] == context['source_schema'] for node in graph['sources'].values())
        assert all(node['schema'] == context['candidate_schema'] for node in graph['nodes'].values()
                   if node['resource_type'] == 'model')
        self.passed('dbt: modelos y pruebas completos sobre la instantánea candidata de esta publicación')
        for name, digest in context['artifact_sha256'].items():
            assert hashlib.sha256((folder / name).read_bytes()).hexdigest() == digest
        assert (folder / 'static_index.html').stat().st_size > 1000
        self.passed('dbt: hashes de manifiesto, resultados, catálogo y documentación HTML verificados')
        repeat_context = self.wh.runs[self.repeat_id]
        assert repeat_context['publication_status'] == 'OK' and repeat_context['tests_passed'] == context['tests_passed']
        assert repeat_context['source_schema'] != context['source_schema']
        assert repeat_context['candidates_cleaned']
        self.passed('dbt: segunda publicación reconstruye y prueba candidatos propios, sin duplicar resultados')

        # Prueba adversa real: alterar solo un candidato de laboratorio y ejecutar la prueba dbt.
        runner = self.wh.dbt
        probe_id = 'dbt_probe_' + uuid.uuid4().hex[:16]
        probe_folder = self.ctx.paths.artifacts / 'dbt' / probe_id
        before_bad_build = self.destination_signature()
        runner.runs[probe_id] = {'candidate_schema': probe_id, 'source_schema': 'stage_' + uuid.uuid4().hex[:16]}
        try:
            runner.command(['run', '--select', 'ventas_mensuales'], probe_folder, 'dw', probe_id)
            with self.dw.begin() as conn:
                conn.execute(text(f'UPDATE TOP (1) [{probe_id}].[ventas_mensuales] SET venta_neta_clp=venta_neta_clp+1'))
            try:
                runner.command(['test', '--select', 'comercial_concilia'], probe_folder, 'dw', probe_id,
                               expected_failure=True)
            except RuntimeError:
                bad_results = json.loads((probe_folder / 'run_results.json').read_text(encoding='utf-8'))
                assert len(bad_results['results']) == 1 and bad_results['results'][0]['status'] == 'fail'
            else:
                raise AssertionError('dbt no detectó el importe alterado')
            try:
                runner.gate(probe_folder, 'dw', probe_id, expected_failure=True)
            except ValueError:
                pass
            else:
                raise AssertionError('El quality gate aceptó un build fallido/incompleto')
        finally:
            runner.cleanup(probe_id)
        assert self.destination_signature() == before_bad_build
        self.passed('dbt: prueba financiera falla ante un peso alterado; gate rechaza y los marts publicados permanecen iguales')
        self.ctx.manifest['validation'].update({'checks_passed': len(self.checks), 'dbt_tests_passed': context['tests_passed'],
                                                'dbt_failure_probe': 'rechazado, candidato eliminado, publicación final sin cambios'})
        self.ctx.write_manifest()

    # -- cell 45 -----------------------------------------------------------------------------
    def snowflake(self):
        st = self.state
        physical, facts = st.dims.physical, st.facts.tables
        sf_tables = self.q("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('dw')")['name']
        assert set(HIERARCHIES).issubset(set(sf_tables)) and len(physical) == 12
        for table, attributes in REMOVED_ATTRIBUTES.items():
            cols = set(self.q('SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(:t)', {'t': 'dw.' + table})['name'])
            assert not cols.intersection(attributes)
            assert set(DIMENSION_LINKS[table]).issubset(cols)
        self.passed('Snowflake: 12 dimensiones físicas y etiquetas normalizadas fuera de Producto/Sucursal')
        for table, links in DIMENSION_LINKS.items():
            for key, parent in links.items():
                count = self.q('''SELECT COUNT(*) n FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
                WHERE fk.parent_object_id=OBJECT_ID(:child) AND fk.referenced_object_id=OBJECT_ID(:parent)
                AND fkc.parent_column_id=COLUMNPROPERTY(OBJECT_ID(:child),:col,'ColumnId')
                AND fk.is_disabled=0 AND fk.is_not_trusted=0''',
                               {'child': 'dw.' + table, 'parent': 'dw.' + parent, 'col': key}).iloc[0, 0]
                assert int(count) == 1
        validate_links(physical)
        self.passed('Snowflake: seis FK físicas habilitadas y confiables entre dimensiones')
        sf_ventas = self.q('''SELECT COUNT_BIG(*) filas,SUM(f.monto_neto) venta
        FROM dw.fact_ventas f JOIN dw.dim_producto p ON f.producto_key=p.producto_key
        JOIN dw.dim_subcategoria sc ON p.subcategoria_key=sc.subcategoria_key
        JOIN dw.dim_categoria c ON sc.categoria_key=c.categoria_key
        JOIN dw.dim_linea l ON c.linea_key=l.linea_key
        JOIN dw.dim_sucursal s ON f.sucursal_key=s.sucursal_key
        JOIN dw.dim_comuna co ON s.comuna_key=co.comuna_key
        JOIN dw.dim_region r ON co.region_key=r.region_key
        JOIN dw.dim_pais pa ON r.pais_key=pa.pais_key''').iloc[0]
        assert int(sf_ventas.filas) == len(facts['fact_ventas'])
        assert int(sf_ventas.venta) == int(facts['fact_ventas'].monto_neto.sum())
        assert len(st.sf_versions) == 2 and st.sf_versions.categoria.nunique() == 2
        self.passed('Snowflake: navegación completa conserva líneas/importes y reclasificación SCD2 conserva el pasado')
        tests = snowflake_selftest()
        self.passed('Snowflake: seis pruebas controladas de homónimos, persistencia, reejecución, round-trip e integridad')
        self.ctx.manifest['validation'].update({'checks_passed': len(self.checks),
                                                'snowflake_dimension_tables': len(physical),
                                                'snowflake_links': sum(len(v) for v in DIMENSION_LINKS.values()),
                                                'snowflake_unit_tests': tests})
        self.ctx.write_manifest()

    def run(self):
        with pipeline_step(self.log, 'pruebas.integridad_y_segunda_publicacion'):
            self.integrity_and_second_publication()
        with pipeline_step(self.log, 'pruebas.dbt_y_falla_controlada'):
            self.dbt_and_failure_probe()
        with pipeline_step(self.log, 'pruebas.snowflake'):
            self.snowflake()
        self.ctx.report.section('8. Controles verificados')
        self.ctx.report.table(pd.DataFrame(self.checks), f'{len(self.checks)} controles aprobados', max_rows=None)
        self.ctx.report.text('**Falla esperada y contenida:** el candidato dbt alterado fue rechazado y eliminado; '
                             'las tablas finales conservaron conteos y contenido. El checksum de SQL Server es un '
                             'control complementario, combinado con PK, FK, conteos y conciliación monetaria.')
        return self.checks
