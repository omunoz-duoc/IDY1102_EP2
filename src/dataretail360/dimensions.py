"""Dimensiones conformadas con historia SCD y normalización snowflake.

SCD1 corrige atributos descriptivos; SCD2 conserva versiones de región/segmento del cliente y
clasificación del producto; SCD3 mantiene zona actual/anterior de la sucursal. Primero se
construye la dimensión lógica (SCD) y luego se normalizan todas sus versiones (snowflake).
"""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from .observability import pipeline_step
from .scd import BASELINE, END, add_unknown, scd_merge, temporal_lookup, validate_scd
from .snowflake import HIERARCHIES, REMOVED_ATTRIBUTES, expand_dimension, normalize_dimensions
from .normalize import norm_series

# Política SCD por dimensión: (nk, sk, tipo2, tipo1, tipo3).
SCD_POLICY = {
    'dim_producto': ('producto_nk', 'producto_key', ['linea', 'categoria', 'subcategoria'], ['producto', 'marca'], []),
    'dim_cliente': ('cliente_nk', 'cliente_key', ['comuna', 'region', 'segmento'], ['acepta_marketing'], []),
    'dim_sucursal': ('sucursal_nk', 'sucursal_key', [], ['sucursal', 'comuna', 'region', 'pais'], ['zona']),
}
CALENDAR_RANGE = ('2023-01-01', '2027-12-31')
CHANNELS = ['DESCONOCIDO', 'TIENDA', 'WEB', 'APP', 'MARKETPLACE']


class History:
    """Historia previa publicada en el DWH; vacía en la primera corrida."""

    def previous(self, name: str) -> pd.DataFrame:
        return pd.DataFrame()

    def hierarchies(self) -> dict[str, pd.DataFrame]:
        return {}


class WarehouseHistory(History):
    """Lee dimensiones y jerarquías ya publicadas en `dw` para continuar su historia."""

    def __init__(self, engine):
        self.engine = engine

    def hierarchies(self):
        result = {}
        for name in HIERARCHIES:
            if sa.inspect(self.engine).has_table(name, schema='dw'):
                with self.engine.connect() as conn:
                    result[name] = pd.read_sql(text(f'SELECT * FROM dw.{name}'), conn)
        return result

    def previous(self, name):
        if sa.inspect(self.engine).has_table(name, schema='dw'):
            with self.engine.connect() as conn:
                previous = pd.read_sql(text(f'SELECT * FROM dw.{name}'), conn)
            link = {'dim_producto': 'subcategoria_key', 'dim_sucursal': 'comuna_key'}.get(name)
            if link and link in previous:
                # Reconstruir atributos para comparar SCD con la misma política anterior.
                previous = expand_dimension(name, previous, self.hierarchies())
            return previous
        return pd.DataFrame()


@dataclass
class Dimensions:
    logical: dict[str, pd.DataFrame]    # representación de trabajo para SCD y lookups
    physical: dict[str, pd.DataFrame]   # modelo normalizado que se almacena y exporta
    store_master: pd.DataFrame
    observed_at: pd.Timestamp

    def merge_policy(self, name, incoming, now=None):
        """Re-aplica la política SCD de `name` sobre la versión actual (prueba de reejecución)."""
        nk, sk, t2, t1, t3 = SCD_POLICY[name]
        return scd_merge(incoming, self.logical[name], nk, sk, t2, t1, t3, now=now or self.observed_at)


def _step(log, name, **fields):
    return pipeline_step(log, name, **fields) if log is not None else nullcontext()


def _scd(log, incoming, previous, name, now):
    nk, sk, t2, t1, t3 = SCD_POLICY[name]
    with _step(log, 'dimensiones.aplicar_scd', object=sk) as step:
        result = scd_merge(incoming, previous, nk, sk, type2=t2, type1=t1, type3=t3, now=now)
        if step is not None:
            step.progress(rows=len(result))
    return add_unknown(result, nk, sk)


def store_master_from(silver_stores: pd.DataFrame) -> pd.DataFrame:
    stores = silver_stores.copy().rename(columns={'codigo_sucursal': 'sucursal_nk', 'nombre_sucursal': 'sucursal'})
    stores['zona'] = norm_series(stores.zona)
    stores['pais'] = 'CHILE'
    store_master = stores[['sucursal_nk', 'sucursal', 'comuna', 'region', 'pais', 'zona']]
    digital = pd.DataFrame([{'sucursal_nk': 'DIGITAL', 'sucursal': 'Digital sin local', 'comuna': 'NO_APLICA',
                             'region': 'NO_APLICA', 'pais': 'CHILE', 'zona': 'NO_APLICA'}])
    return pd.concat([store_master, digital], ignore_index=True)


def calendar_dimension() -> pd.DataFrame:
    calendar = pd.DataFrame({'fecha': pd.date_range(*CALENDAR_RANGE)})
    calendar['fecha_key'] = calendar.fecha.dt.strftime('%Y%m%d').astype('int64')
    calendar['anio'] = calendar.fecha.dt.year
    calendar['mes'] = calendar.fecha.dt.month
    calendar['trimestre'] = calendar.fecha.dt.quarter
    calendar['dia'] = calendar.fecha.dt.day
    calendar['semana_iso'] = calendar.fecha.dt.isocalendar().week.astype('int64')
    calendar['anio_iso'] = calendar.fecha.dt.isocalendar().year.astype('int64')
    unknown = pd.DataFrame([{'fecha': BASELINE, 'fecha_key': 0, 'anio': 0, 'mes': 0, 'trimestre': 0,
                             'dia': 0, 'semana_iso': 0, 'anio_iso': 0}])
    return pd.concat([unknown, calendar], ignore_index=True)


def campaign_dimension(campaigns: pd.DataFrame) -> pd.DataFrame:
    dim = campaigns[['id_campania', 'codigo_campania', 'nombre_campania', 'tipo_campania', 'fecha_inicio',
                     'fecha_fin', 'presupuesto_clp', 'calidad_campania']].copy()
    dim = dim.rename(columns={'id_campania': 'campania_key'})
    unknown = pd.DataFrame([{'campania_key': 0, 'codigo_campania': 'SIN_CAMPANIA',
                             'nombre_campania': 'Sin campaña o no homologada', 'tipo_campania': 'NO_APLICA',
                             'fecha_inicio': BASELINE, 'fecha_fin': BASELINE, 'presupuesto_clp': 0,
                             'calidad_campania': 'NO_APLICA'}])
    return pd.concat([unknown, dim], ignore_index=True)


def build_dimensions(silver, history: History | None = None, observed_at: pd.Timestamp | None = None,
                     log=None) -> Dimensions:
    from .scd import observed_now
    history = history or History()
    now = observed_at if observed_at is not None else observed_now()
    store_master = store_master_from(silver.tables['pos_sucursales'])
    with _step(log, 'dimensiones.construir_scd'):
        dims = {}
        dims['dim_producto'] = _scd(log, silver.products.product_master, history.previous('dim_producto'),
                                    'dim_producto', now)
        dims['dim_cliente'] = _scd(log, silver.identities.client_master, history.previous('dim_cliente'),
                                   'dim_cliente', now)
        dims['dim_sucursal'] = _scd(log, store_master, history.previous('dim_sucursal'), 'dim_sucursal', now)
        # Dimensiones estáticas: calendario y canales con claves estables.
        dims['dim_fecha'] = calendar_dimension()
        dims['dim_canal'] = pd.DataFrame({'canal_key': list(range(len(CHANNELS))), 'canal': CHANNELS})
        dims['dim_campania'] = campaign_dimension(silver.campaigns)
        for name, (nk, sk, *_) in SCD_POLICY.items():
            validate_scd(dims[name], nk, sk)
    with _step(log, 'dimensiones.normalizar_snowflake', heartbeat=False):
        # Primero SCD lógico, luego normalización de todas las versiones: no reiniciar claves.
        physical = normalize_dimensions(dims, history.hierarchies())
        for name in REMOVED_ATTRIBUTES:
            restored = expand_dimension(name, physical[name], physical)
            key = {'dim_producto': 'producto_key', 'dim_sucursal': 'sucursal_key'}[name]
            pd.testing.assert_frame_equal(dims[name].sort_values(key).reset_index(drop=True),
                                          restored[dims[name].columns].sort_values(key).reset_index(drop=True),
                                          check_dtype=False)
    return Dimensions(logical=dims, physical=physical, store_master=store_master, observed_at=now)


def scd_selftest() -> pd.DataFrame:
    """Prueba aislada: corrección SCD1, historia SCD2 y anterior/actual SCD3.

    Ningún cambio ficticio se escribe en las tablas del negocio.
    """
    demo = pd.DataFrame([{'nk': 'CASO_PRUEBA', 'nombre': 'Texto con error', 'region': 'METROPOLITANA', 'zona': 'CENTRO'}])
    policy = dict(type2=['region'], type1=['nombre'], type3=['zona'])
    d1 = scd_merge(demo, pd.DataFrame(), 'nk', 'sk', now=pd.Timestamp('2026-01-01'), **policy)
    demo2 = demo.assign(nombre='Texto corregido', region='BIOBIO', zona='SUR')
    d2 = scd_merge(demo2, d1, 'nk', 'sk', now=pd.Timestamp('2026-06-01'), **policy)
    assert len(d2) == 2 and d2.nombre.eq('Texto corregido').all()
    assert d2.loc[d2.sk.eq(1), 'region'].iloc[0] == 'METROPOLITANA'
    assert d2.loc[d2.sk.eq(1), 'zona_anterior'].iloc[0] == 'CENTRO'
    d3 = scd_merge(demo2, d2, 'nk', 'sk', now=pd.Timestamp('2026-06-02'), **policy)
    assert len(d3) == 2
    lookup_demo = pd.DataFrame({'nk': ['CASO_PRUEBA', 'CASO_PRUEBA'], 'fecha': pd.to_datetime(['2026-05-31', '2026-06-01'])})
    assert list(temporal_lookup(lookup_demo, lookup_demo.nk, lookup_demo.fecha, d2, 'nk', 'sk')) == [1, 2]
    validate_scd(d2, 'nk', 'sk')
    return d2


def reclassification_demo(store_dimension: pd.DataFrame) -> pd.DataFrame:
    """Cambio SCD2 controlado: la misma identidad conserva su rama anterior al reclasificarse.

    Solo se usan registros ficticios en memoria; no se escriben en la base del negocio.
    """
    nk, sk, t2, t1, _ = SCD_POLICY['dim_producto']
    sf_input = pd.DataFrame([{'producto_nk': 'DEMO_PRODUCTO', 'producto': 'Producto ficticio', 'marca': 'DEMO',
                              'linea': 'HOGAR', 'categoria': 'COCINA', 'subcategoria': 'ACCESORIOS'}])
    sf_v1 = add_unknown(scd_merge(sf_input, pd.DataFrame(), nk, sk, type2=t2, type1=t1, now=pd.Timestamp('2026-01-01')), nk, sk)
    sf_first = normalize_dimensions({'dim_producto': sf_v1, 'dim_sucursal': store_dimension})
    sf_changed = sf_input.assign(categoria='ORGANIZACION')
    sf_v2 = scd_merge(sf_changed, sf_v1, nk, sk, type2=t2, type1=t1, now=pd.Timestamp('2026-06-01'))
    sf_second = normalize_dimensions({'dim_producto': sf_v2, 'dim_sucursal': store_dimension}, sf_first)
    sf_versions = expand_dimension('dim_producto', sf_second['dim_producto'], sf_second)
    sf_versions = sf_versions.loc[sf_versions.producto_nk.eq('DEMO_PRODUCTO')].sort_values('producto_key')
    assert len(sf_versions) == 2 and sf_versions.categoria.tolist() == ['COCINA', 'ORGANIZACION']
    assert sf_second['dim_producto'].loc[lambda d: d.producto_nk.eq('DEMO_PRODUCTO'), 'subcategoria_key'].nunique() == 2
    for table, (key, _parent, _label) in HIERARCHIES.items():
        original = sf_first[table]
        kept = sf_second[table].loc[lambda d: d[key].isin(original[key])].reset_index(drop=True)
        pd.testing.assert_frame_equal(original, kept)
    sf_lookup = pd.DataFrame({'producto_nk': ['DEMO_PRODUCTO'] * 2, 'fecha': pd.to_datetime(['2026-05-31', '2026-06-01'])})
    assert list(temporal_lookup(sf_lookup, sf_lookup.producto_nk, sf_lookup.fecha, sf_v2, nk, sk)) == sf_versions.producto_key.tolist()
    return sf_versions


def report_dimensions(report, dims: Dimensions, scd_demo: pd.DataFrame, sf_versions: pd.DataFrame) -> None:
    report.section('3. Dimensiones e historia temporal')
    report.table(pd.DataFrame([{'dimension': n, 'filas': len(d)} for n, d in dims.logical.items()]),
                 'Dimensiones preparadas')
    report.table(scd_demo, 'Prueba SCD controlada (historia, límite temporal, corrección y reejecución)')
    report.table(pd.DataFrame([{'tabla': name, 'filas': len(frame), 'columnas': ', '.join(frame.columns)}
                               for name, frame in dims.physical.items()
                               if name in HIERARCHIES or name in REMOVED_ATTRIBUTES]),
                 'Snowflake: tablas físicas de Producto y Sucursal')
    report.table(sf_versions[['producto_key', 'producto_nk', 'categoria', 'subcategoria', 'valido_desde',
                              'valido_hasta', 'es_actual']],
                 'Demostración: reclasificación sin reescribir el pasado')
