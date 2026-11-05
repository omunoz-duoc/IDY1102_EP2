"""Contratos del destino: PK, FK y grano explícitos, sin usar to_sql(replace)."""
from __future__ import annotations

from contextlib import nullcontext

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from ..observability import pipeline_step
from ..snowflake import DIMENSION_LINKS, HIERARCHIES

FACT_PK = {'fact_ventas': ['venta_nk'], 'fact_devoluciones': ['devolucion_nk'],
           'fact_inventario': ['fecha_key', 'producto_key', 'sucursal_key'],
           'fact_marketing': ['envio_nk'], 'fact_web': ['sesion_nk'], 'fact_reposicion': ['orden_nk']}
DIM_PK = {'dim_fecha': 'fecha_key', 'dim_producto': 'producto_key', 'dim_cliente': 'cliente_key',
          'dim_sucursal': 'sucursal_key', 'dim_canal': 'canal_key', 'dim_campania': 'campania_key'}
DIM_PK.update({name: spec[0] for name, spec in HIERARCHIES.items()})
FK_TARGET = {v: (k, v) for k, v in DIM_PK.items()}
FK_TARGET['fecha_recepcion_key'] = ('dim_fecha', 'fecha_key')
SCD_NK = {'dim_producto': 'producto_nk', 'dim_cliente': 'cliente_nk', 'dim_sucursal': 'sucursal_nk'}


def validate_destination(frames: dict, dimensions: dict) -> None:
    """Quality gate previo a publicar: grano, integridad referencial y medidas no negativas."""
    for name, frame in frames.items():
        if frame[FACT_PK[name]].isna().any().any() or frame.duplicated(FACT_PK[name]).any():
            raise ValueError(f'Grano inválido: {name}')
        for col, (target, key) in FK_TARGET.items():
            if col in frame and not frame[col].isin(dimensions[target][key]).all():
                raise ValueError(f'Integridad fallida: {name}.{col}')
        for col in ['monto_neto', 'monto_neto_devuelto', 'monto_con_iva', 'cantidad']:
            if col in frame and (frame[col].isna().any() or frame[col].lt(0).any()):
                raise ValueError(f'Medida inválida: {name}.{col}')


def sqltype(series: pd.Series):
    if pd.api.types.is_bool_dtype(series):
        return sa.Boolean()
    if pd.api.types.is_integer_dtype(series):
        return sa.BigInteger()
    if pd.api.types.is_float_dtype(series):
        return sa.Float()
    if pd.api.types.is_datetime64_any_dtype(series):
        return sa.DateTime()
    maxlen = series.dropna().astype(str).str.len().max()
    if pd.notna(maxlen) and maxlen > 400:
        raise ValueError('Texto excede contrato del destino')
    return sa.Unicode(200 if pd.isna(maxlen) or maxlen <= 200 else 400)


def build_metadata(physical: dict, facts: dict, log=None) -> sa.MetaData:
    """Tablas `dw.*` con PK, FK, únicos jerárquicos, índice de versión vigente y vigencia SCD."""
    metadata = sa.MetaData()
    for name, frame in {**physical, **facts}.items():
        with (pipeline_step(log, 'dw.cargar_y_conciliar', object=name, rows_total=len(frame))
              if log is not None else nullcontext()) as step:
            _define_table(metadata, name, frame, facts)
            if step is not None:
                step.progress(rows=int(len(frame)))
    sa.Table('ejecuciones', metadata,
             sa.Column('run_id', sa.String(40), primary_key=True), sa.Column('inicio_utc', sa.DateTime()),
             sa.Column('fin_utc', sa.DateTime()), sa.Column('estado', sa.String(20)),
             sa.Column('tablas', sa.Integer()), sa.Column('filas_hechos', sa.BigInteger()), schema='ctl')
    return metadata


def _define_table(metadata, name, frame, facts):
    pk = [DIM_PK[name]] if name in DIM_PK else FACT_PK[name]
    cols = []
    for col in frame:
        refs = []
        if name in facts and col in FK_TARGET:
            dest, key = FK_TARGET[col]
            refs = [sa.ForeignKey(f'dw.{dest}.{key}')]
        if col in DIMENSION_LINKS.get(name, {}):
            parent = DIMENSION_LINKS[name][col]
            refs = [sa.ForeignKey(f'dw.{parent}.{col}')]
        cols.append(sa.Column(col, sqltype(frame[col]), *refs, primary_key=col in pk, autoincrement=False,
                              nullable=False if col in pk or refs or name in HIERARCHIES else True))
    table = sa.Table(name, metadata, *cols, schema='dw')
    if name in HIERARCHIES:
        _, parent, label = HIERARCHIES[name]
        table.append_constraint(sa.UniqueConstraint(*(([parent] if parent else []) + [label]),
                                                    name='uq_' + name + '_padre_nombre'))
    if 'es_actual' in frame:
        nk = SCD_NK[name]
        sa.Index('ux_' + name + '_vigente', table.c[nk], unique=True, mssql_where=text('es_actual = 1'))
        sa.CheckConstraint('valido_desde < valido_hasta', table=table, name='ck_' + name + '_vigencia')
    if name in facts:
        sa.Index('ix_' + name + '_fecha', table.c.fecha_key)
        if 'producto_key' in frame:
            sa.Index('ix_' + name + '_producto', table.c.producto_key)


GOLD_MONTHLY_DDL = '''IF OBJECT_ID('dw.gold_ventas_mes','U') IS NULL
    CREATE TABLE dw.gold_ventas_mes(anio bigint NOT NULL,mes bigint NOT NULL,canal_key bigint NOT NULL,
    venta_sin_devoluciones bigint NOT NULL,documentos bigint NOT NULL,unidades bigint NOT NULL,
    PRIMARY KEY(anio,mes,canal_key))'''


def safe_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """pyodbc requiere datetime Python sin tz y valores nulos representables."""
    result = frame.copy()
    for col in result:
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = pd.to_datetime(result[col]).astype('datetime64[ns]')
    return result
