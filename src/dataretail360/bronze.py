"""Extracción heterogénea y capa Bronze.

Se leen PostgreSQL, CSV y Excel mediante adaptadores explícitos con un contrato de columnas
compartido. Los archivos originales se copian a Bronze por SHA256 sin modificar data/raw.
El hash del archivo identifica sus bytes; `hash`/`_batch` identifican la instantánea tabular
decodificada. Un archivo, hoja o columna obligatoria ausente detiene la ingestión. Solo se
reintentan fallas transitorias de conexión SQL; no se sustituye un archivo por una tabla SQL.
"""
from __future__ import annotations

import hashlib
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from .context import RunContext
from .contracts import CONTRACT, CONTROL_SCHEMA, CONTROL_TABLES, FILE_SOURCES, KEYS, RAW_SCHEMA, SOURCES
from .observability import event, pipeline_step, trace_step


@dataclass
class BronzeSnapshot:
    raw: dict[str, pd.DataFrame]
    inventory: pd.DataFrame
    schema: pd.DataFrame
    controls: dict[str, pd.DataFrame]
    checks: list[dict]

    @property
    def customer_cutoff(self) -> pd.Timestamp:
        """`fecha_fin` del contrato del dataset: cobertura fiable de compras."""
        return pd.Timestamp(self.controls['config_generacion'].set_index('parametro').loc['fecha_fin', 'valor'])


def validate_columns(name: str, frame: pd.DataFrame) -> None:
    """Contrato de columnas compartido por SQL, CSV y Excel; la calidad se valida en Silver."""
    if frame.columns.duplicated().any():
        raise ValueError(f'Columnas duplicadas: {name}')
    missing = set(CONTRACT[name]) - set(frame.columns)
    if missing:
        raise ValueError(f'Contrato roto: {name}; faltan columnas {sorted(missing)}')


def read_file_source(ctx: RunContext, name: str, spec: dict):
    """Lee bytes congelados y conserva el archivo original; no modifica data/raw."""
    root = ctx.paths.home
    source_path = root / spec['ubicacion']
    if not source_path.is_file():
        raise FileNotFoundError(f'Fuente requerida ausente: {spec["ubicacion"]}')
    content = source_path.read_bytes()
    file_sha = hashlib.sha256(content).hexdigest()
    original = ctx.paths.bronze / name / 'originales' / f'{file_sha}{source_path.suffix.lower()}'
    original.parent.mkdir(parents=True, exist_ok=True)
    if original.exists():
        if hashlib.sha256(original.read_bytes()).hexdigest() != file_sha:
            raise ValueError(f'Copia Bronze alterada: {name}')
    else:
        temp = original.with_name(original.name + '.' + ctx.run_id + '.tmp')
        temp.write_bytes(content)
        temp.replace(original)
    if spec['tipo_fuente'] == 'CSV':
        frame = pd.read_csv(BytesIO(content), sep=spec['sep'], encoding=spec['encoding'],
                            dtype='string', keep_default_na=False, on_bad_lines='error')
    elif spec['tipo_fuente'] == 'EXCEL':
        with pd.ExcelFile(BytesIO(content), engine='openpyxl') as book:
            if spec['hoja_entrada'] not in book.sheet_names:
                raise ValueError(f'Hoja requerida ausente en {name}: {spec["hoja_entrada"]}')
            frame = pd.read_excel(book, sheet_name=spec['hoja_entrada'],
                                  dtype='string', keep_default_na=False)
    else:
        raise ValueError(f'Adaptador de archivo desconocido: {name}')
    # Se conserva el texto leído; las conversiones de negocio se realizan en Silver.
    # El XLSX original conserva además los tipos y formatos nativos de sus celdas.
    validate_columns(name, frame)
    return frame, {'sha256_archivo': file_sha, 'bytes_archivo': len(content),
                   'original_bronze': str(original.relative_to(root))}


def _extract(ctx: RunContext):
    """Congela archivos y obtiene una instantánea consistente de las fuentes SQL."""
    log = ctx.log
    # Un error de archivo, hoja o contrato detiene la ejecución antes de consultar datos SQL.
    file_inputs = {}
    for name, spec in FILE_SOURCES.items():
        with pipeline_step(log, 'bronze.archivo', object=name, source_type=spec['tipo_fuente']) as _file_step:
            file_inputs[name] = read_file_source(ctx, name, spec)
            _file_step.progress(rows=len(file_inputs[name][0]))
    sql_names = [name for name, spec in SOURCES.items() if spec['tipo_fuente'] == 'POSTGRESQL']
    schema_query = text(f'''SELECT table_name,column_name,data_type
        FROM information_schema.columns WHERE table_schema='{RAW_SCHEMA}'
        AND table_name IN :names ORDER BY table_name,ordinal_position''').bindparams(
        sa.bindparam('names', expanding=True))
    for attempt in range(3):
        try:
            raw, inventory = {}, []
            with ctx.pg.connect().execution_options(isolation_level='REPEATABLE READ') as conn:
                with conn.begin():
                    conn.execute(text('SET TRANSACTION READ ONLY'))
                    schema = pd.read_sql(schema_query, conn, params={'names': sql_names})
                    schema['tipo_fuente'] = 'POSTGRESQL'
                    for name, key in KEYS.items():
                        with pipeline_step(log, 'bronze.conjunto', object=name,
                                           source_type=SOURCES[name]['tipo_fuente'], attempt=attempt + 1) as _source_step:
                            spec = SOURCES[name]
                            if spec['tipo_fuente'] == 'POSTGRESQL':
                                cols = schema.loc[schema.table_name.eq(name), 'column_name'].tolist()
                                if not set(CONTRACT[name]).issubset(cols):
                                    raise ValueError(f'Contrato roto en SQL: {name}')
                                # Nombres de tabla y claves de orden proceden de listas fijas.
                                frame = pd.read_sql(text(f'SELECT * FROM {RAW_SCHEMA}.{name} ORDER BY ' + ','.join(key)), conn)
                                file_meta = {'sha256_archivo': '', 'bytes_archivo': 0, 'original_bronze': ''}
                            else:
                                frame, file_meta = file_inputs[name]
                                frame = frame.copy()
                                schema = pd.concat([schema, pd.DataFrame({
                                    'table_name': name, 'column_name': frame.columns,
                                    'data_type': [str(t) for t in frame.dtypes],
                                    'tipo_fuente': spec['tipo_fuente']})], ignore_index=True)
                            validate_columns(name, frame)
                            digest = hashlib.sha256(pd.util.hash_pandas_object(frame, index=False).values.tobytes()).hexdigest()
                            path = ctx.paths.bronze / name / f'{digest}.parquet'
                            path.parent.mkdir(exist_ok=True)
                            if not path.exists():
                                temp = path.with_name(path.name + '.' + ctx.run_id + '.tmp')
                                frame.to_parquet(temp, index=False)
                                temp.replace(path)
                            source_columns = len(frame.columns)
                            # En archivos, 0 corresponde a la primera fila de datos, sin encabezado.
                            frame['_src_row'] = np.arange(len(frame), dtype='int64')
                            frame['_batch'] = digest
                            raw[name] = frame
                            inventory.append({'tabla': name, 'tipo_fuente': spec['tipo_fuente'],
                                              'ubicacion': spec['ubicacion'], 'hoja_entrada': spec['hoja_entrada'],
                                              'filas': len(frame), 'columnas': source_columns, 'hash': digest,
                                              'archivo': str(path.relative_to(ctx.paths.home)), **file_meta})
                            log.info('extract fuente=%s tipo=%s filas=%d', name, spec['tipo_fuente'], len(frame))
                            _source_step.progress(rows=len(frame))
                    with pipeline_step(log, 'bronze.metadatos_control'):
                        controls = {n: pd.read_sql(text(f'SELECT * FROM {CONTROL_SCHEMA}.{n}'), conn)
                                    for n in CONTROL_TABLES}
            return raw, pd.DataFrame(inventory), schema, controls
        except sa.exc.OperationalError:
            event(log, 'source_retry', 'Falló la conexión SQL en este intento.', level=logging.WARNING,
                  attempt=attempt + 1,
                  action='Reintentar con backoff; si se agotan tres intentos, verificar conexión al origen.')
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
        except Exception as exc:
            log.error('Extracción detenida tipo=%s', type(exc).__name__)
            raise


def verify_readers(ctx: RunContext, raw: dict, inventory: pd.DataFrame) -> list[dict]:
    """Evidencia de los tres adaptadores y pruebas de fallas sin alterar archivos de entrada."""
    checks = []
    assert inventory.groupby('tipo_fuente').size().to_dict() == {'CSV': 1, 'EXCEL': 1, 'POSTGRESQL': 14}
    assert set(inventory.tabla) == set(CONTRACT)
    checks.append({'prueba': 'Tres adaptadores reales: 14 fuentes SQL, un CSV y un Excel', 'resultado': 'OK'})
    root = ctx.paths.home
    for name, spec in FILE_SOURCES.items():
        row = inventory.set_index('tabla').loc[name]
        assert row.tipo_fuente == spec['tipo_fuente']
        assert row.ubicacion == spec['ubicacion']
        assert hashlib.sha256((root / row.original_bronze).read_bytes()).hexdigest() == row.sha256_archivo
        assert hashlib.sha256((root / spec['ubicacion']).read_bytes()).hexdigest() == row.sha256_archivo
        assert len(raw[name]) == int(row.filas)
    checks.append({'prueba': 'Archivos originales intactos y copias Bronze con SHA256 verificable', 'resultado': 'OK'})
    try:
        read_file_source(ctx, 'mkt_campanias', {**FILE_SOURCES['mkt_campanias'],
                         'ubicacion': f'data/raw/marketing/ausente_{uuid.uuid4().hex}.csv'})
    except FileNotFoundError:
        checks.append({'prueba': 'Archivo ausente detiene la ingestión sin fallback a PostgreSQL', 'resultado': 'OK'})
    else:
        raise AssertionError('Se aceptó un archivo ausente')
    try:
        validate_columns('mkt_campanias', raw['mkt_campanias'].drop(columns='id_campania'))
    except ValueError:
        checks.append({'prueba': 'Contrato detecta una columna obligatoria ausente en CSV', 'resultado': 'OK'})
    else:
        raise AssertionError('Se aceptó un CSV sin clave requerida')
    return checks


def extract(ctx: RunContext) -> BronzeSnapshot:
    """Etapa Bronze completa: instantánea, manifiesto, esquema y evidencia de lectores."""
    raw, inventory, source_schema, controls = trace_step(ctx.log, 'bronze.instantanea')(_extract)(ctx)
    ctx.manifest.update({
        'run_id': ctx.run_id, 'extracted_at_utc': datetime.now(timezone.utc).isoformat(),
        'consistencia': 'Instantánea transaccional SQL y archivos congelados por SHA256; no hay transacción global entre fuentes.',
        'sources': inventory.to_dict('records'),
        'observability': {'human_log': f'logs/{ctx.run_id}.log', 'jsonl_log': f'logs/{ctx.run_id}.jsonl',
                          'heartbeat_seconds': 30, 'slow_step_seconds': 300}})
    ctx.write_manifest()
    source_schema.to_parquet(ctx.paths.control / 'source_schema.parquet', index=False)
    ctx.report.section('1. Extracción heterogénea y capa Bronze')
    ctx.report.table(inventory[['tabla', 'tipo_fuente', 'filas', 'columnas', 'ubicacion']], 'Inventario de fuentes')
    ctx.report.text(f'Filas extraídas: **{int(inventory.filas.sum()):,}**')
    with pipeline_step(ctx.log, 'pruebas.lectores', heartbeat=False):
        checks = verify_readers(ctx, raw, inventory)
    ctx.report.table(pd.DataFrame(checks), 'Evidencia de los lectores de entrada')
    ctx.report.table(inventory.loc[inventory.tipo_fuente.ne('POSTGRESQL'),
                                   ['tabla', 'tipo_fuente', 'hoja_entrada', 'sha256_archivo', 'original_bronze']])
    return BronzeSnapshot(raw=raw, inventory=inventory, schema=source_schema, controls=controls, checks=checks)
