"""Arranque de fuentes: lo mínimo que el pipeline necesita para ejecutarse.

1. PostgreSQL: esquema `dr360_raw` con 14 tablas operacionales y `dr360_control` con
   `config_generacion` (parámetro `fecha_fin`), `definiciones_kpi_por_area` y
   `problemas_calidad_inyectados`.
2. Archivos: `data/raw/marketing/campanias.csv` y `data/raw/legacy/reportes_ventas.xlsx`.
3. SQL Server: la base destino (`MSSQL_DATABASE`) debe existir.

Tres caminos:
- `demo`: dataset sintético determinista (no requiere datos externos).
- `export`: congela las fuentes actuales en un *paquete de semilla* portable (Parquet + DDL
  de columnas + archivos originales + SHA256).
- `load`: reconstruye las fuentes desde un paquete en cualquier máquina.

Salvaguardas: nunca se sobrescriben fuentes existentes sin `replace=True`; el seed demo omite
(sin error) un origen que ya contiene datos no demo.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from ..bronze import TEXT_TYPES
from ..contracts import CONTRACT, CONTROL_SCHEMA, CONTROL_TABLES, FILE_SOURCES, KEYS, RAW_SCHEMA, SQL_SOURCES
from ..settings import Paths, dw_database, postgres_engine, sqlserver_engine
from . import synthetic

BUNDLE_FORMAT = 'dataretail360-seed/1'
MARKER_TABLE = f'{CONTROL_SCHEMA}.demo_version'


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _log(message: str) -> None:
    print(message, flush=True)


# -- destino ---------------------------------------------------------------------------------
def ensure_target_database() -> bool:
    """Crea la base destino de SQL Server si no existe. Devuelve True si la creó."""
    name = dw_database()
    if not name.replace('_', '').isalnum():
        raise ValueError('Nombre de base destino no permitido: ' + name)
    master = sqlserver_engine('master')
    try:
        with master.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
            if conn.scalar(text('SELECT DB_ID(:n)'), {'n': name}) is not None:
                return False
            conn.execute(text(f'CREATE DATABASE [{name}]'))
            return True
    finally:
        master.dispose()


# -- estado del origen ---------------------------------------------------------------------------
def _raw_tables(conn) -> list[str]:
    return list(conn.scalars(text('SELECT table_name FROM information_schema.tables WHERE table_schema=:s'),
                             {'s': RAW_SCHEMA}))


def _demo_version(conn):
    exists = conn.scalar(text("SELECT to_regclass(:t) IS NOT NULL"), {'t': MARKER_TABLE})
    return conn.scalar(text(f'SELECT version FROM {MARKER_TABLE}')) if exists else None


def _drop_sources(conn) -> None:
    conn.execute(text(f'DROP SCHEMA IF EXISTS {RAW_SCHEMA} CASCADE'))
    conn.execute(text(f'DROP SCHEMA IF EXISTS {CONTROL_SCHEMA} CASCADE'))


def _files_present(paths: Paths) -> list[Path]:
    return [paths.home / spec['ubicacion'] for spec in FILE_SOURCES.values()
            if (paths.home / spec['ubicacion']).exists()]


# -- demo ------------------------------------------------------------------------------------
def seed_demo(paths: Paths | None = None, replace: bool = False) -> str:
    """Siembra el dataset sintético. Idempotente: no reescribe una semilla demo existente."""
    paths = paths or Paths.from_env()
    dataset = synthetic.frames()
    pg = postgres_engine()
    try:
        with pg.begin() as conn:
            version = _demo_version(conn)
            existing = _raw_tables(conn)
            if replace:
                _drop_sources(conn)
                version, existing = None, []
            if version and version != synthetic.VERSION:
                raise RuntimeError(f'Semilla demo {version} incompatible con {synthetic.VERSION}; usar --replace.')
            if existing and not version:
                _log('Origen PostgreSQL con datos no demo: se conserva sin cambios (no se siembra).')
                return 'skipped'
            if not version:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}'))
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}'))
                for name in SQL_SOURCES:
                    dataset[name].to_sql(name, conn, schema=RAW_SCHEMA, index=False, if_exists='fail')
                for name, frame in synthetic.controls().items():
                    frame.to_sql(name, conn, schema=CONTROL_SCHEMA, index=False, if_exists='fail')
                conn.execute(text(f'CREATE TABLE {MARKER_TABLE} (version text PRIMARY KEY)'))
                conn.execute(text(f'INSERT INTO {MARKER_TABLE} VALUES (:v)'), {'v': synthetic.VERSION})
                _log(f'PostgreSQL: {len(SQL_SOURCES)} tablas {RAW_SCHEMA} y {len(CONTROL_TABLES)} de control sembradas.')
            else:
                _log(f'PostgreSQL: semilla demo {version} ya presente.')
    finally:
        pg.dispose()
    marker = paths.data / '.demo-version'
    present = _files_present(paths)
    if replace:
        for path in present:
            path.unlink()
        marker.unlink(missing_ok=True)
        present = []
    if marker.exists() and marker.read_text().strip() != synthetic.VERSION:
        raise RuntimeError('Archivos demo de otra versión; usar --replace.')
    if not marker.exists():
        if present:
            _log('Archivos raw existentes sin marcador demo: se conservan sin cambios.')
        else:
            synthetic.write_files(paths.home, dataset)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(synthetic.VERSION, encoding='utf-8')
            _log('Archivos CSV y Excel de la demo escritos en data/raw.')
    if ensure_target_database():
        _log(f'SQL Server: base {dw_database()} creada.')
    return 'seeded'


# -- paquetes de semilla --------------------------------------------------------------------------
def _column_ddl(row) -> str:
    kind = row.data_type
    if kind == 'character varying':
        kind = f'varchar({int(row.character_maximum_length)})' if pd.notna(row.character_maximum_length) else 'varchar'
    elif kind == 'character':
        kind = f'char({int(row.character_maximum_length)})' if pd.notna(row.character_maximum_length) else 'char'
    elif kind == 'numeric' and pd.notna(row.numeric_precision):
        kind = f'numeric({int(row.numeric_precision)},{int(row.numeric_scale or 0)})'
    elif kind in ('USER-DEFINED', 'ARRAY'):
        kind = 'text'
    return f'"{row.column_name}" {kind}' + ('' if row.is_nullable == 'YES' else ' NOT NULL')


def export_bundle(target: Path, paths: Paths | None = None) -> Path:
    """Congela las fuentes actuales (PostgreSQL + archivos) en un paquete portable verificable."""
    paths = paths or Paths.from_env()
    target = Path(target)
    if target.exists() and any(target.iterdir()):
        raise FileExistsError(f'El destino del paquete no está vacío: {target}')
    manifest = {'format': BUNDLE_FORMAT, 'created_utc': datetime.now(timezone.utc).isoformat(),
                'tables': {}, 'files': {}}
    pg = postgres_engine()
    try:
        with pg.connect().execution_options(isolation_level='REPEATABLE READ') as conn, conn.begin():
            conn.execute(text('SET TRANSACTION READ ONLY'))
            columns = pd.read_sql(text('''SELECT table_schema,table_name,column_name,data_type,
                character_maximum_length,numeric_precision,numeric_scale,is_nullable,ordinal_position
                FROM information_schema.columns WHERE table_schema IN (:raw,:ctl)
                ORDER BY table_schema,table_name,ordinal_position'''), conn,
                                  params={'raw': RAW_SCHEMA, 'ctl': CONTROL_SCHEMA})
            wanted = [(RAW_SCHEMA, n) for n in SQL_SOURCES] + [(CONTROL_SCHEMA, n) for n in CONTROL_TABLES]
            for schema, name in wanted:
                cols = columns.loc[columns.table_schema.eq(schema) & columns.table_name.eq(name)]
                if cols.empty:
                    raise ValueError(f'Fuente requerida ausente en PostgreSQL: {schema}.{name}')
                order = KEYS.get(name, []) + [c for c in cols.column_name if c not in KEYS.get(name, [])]
                kinds = cols.set_index('column_name').data_type
                frame = pd.read_sql(text(f'SELECT * FROM {schema}.{name} ORDER BY ' + ','.join(
                    f'"{c}" COLLATE "C"' if kinds[c] in TEXT_TYPES else f'"{c}"' for c in order)), conn)
                out = target / schema / f'{name}.parquet'
                out.parent.mkdir(parents=True, exist_ok=True)
                frame.to_parquet(out, index=False)
                manifest['tables'][f'{schema}.{name}'] = {
                    'rows': len(frame), 'sha256': _sha256(out),
                    'ddl': [_column_ddl(r) for r in cols.itertuples(index=False)]}
    finally:
        pg.dispose()
    for name, spec in FILE_SOURCES.items():
        source = paths.home / spec['ubicacion']
        if not source.is_file():
            raise FileNotFoundError(f'Fuente requerida ausente: {spec["ubicacion"]}')
        out = target / 'files' / spec['ubicacion']
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, out)
        manifest['files'][spec['ubicacion']] = {'sha256': _sha256(out), 'bytes': out.stat().st_size}
    (target / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    _log(f'Paquete de semilla escrito en {target}: {len(manifest["tables"])} tablas y {len(manifest["files"])} archivos.')
    return target


def verify_bundle(source: Path) -> dict:
    source = Path(source)
    manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
    if manifest.get('format') != BUNDLE_FORMAT:
        raise ValueError(f'Formato de paquete no soportado: {manifest.get("format")}')
    for qualified, meta in manifest['tables'].items():
        schema, name = qualified.split('.')
        if _sha256(source / schema / f'{name}.parquet') != meta['sha256']:
            raise ValueError(f'Paquete alterado: {qualified}')
    for relative, meta in manifest['files'].items():
        if _sha256(source / 'files' / relative) != meta['sha256']:
            raise ValueError(f'Paquete alterado: {relative}')
    missing = ({f'{RAW_SCHEMA}.{n}' for n in SQL_SOURCES} | {f'{CONTROL_SCHEMA}.{n}' for n in CONTROL_TABLES}) \
        - set(manifest['tables'])
    if missing or set(manifest['files']) != {s['ubicacion'] for s in FILE_SOURCES.values()}:
        raise ValueError(f'Paquete incompleto: {sorted(missing)}')
    return manifest


def load_bundle(source: Path, paths: Paths | None = None, replace: bool = False) -> None:
    """Reconstruye PostgreSQL y data/raw desde un paquete; recrea los tipos de columna exportados."""
    paths = paths or Paths.from_env()
    source = Path(source)
    manifest = verify_bundle(source)
    present = _files_present(paths)
    if present and not replace:
        raise FileExistsError('Ya existen archivos raw; usar --replace para sustituirlos.')
    pg = postgres_engine()
    try:
        with pg.begin() as conn:
            if (_raw_tables(conn) or _demo_version(conn)) and not replace:
                raise RuntimeError('El origen PostgreSQL ya contiene datos; usar --replace para sustituirlos.')
            _drop_sources(conn)
            conn.execute(text(f'CREATE SCHEMA {RAW_SCHEMA}'))
            conn.execute(text(f'CREATE SCHEMA {CONTROL_SCHEMA}'))
            for qualified, meta in manifest['tables'].items():
                schema, name = qualified.split('.')
                conn.execute(text(f'CREATE TABLE {schema}.{name} (' + ', '.join(meta['ddl']) + ')'))
                frame = pd.read_parquet(source / schema / f'{name}.parquet')
                if len(frame) != meta['rows']:
                    raise ValueError(f'Conteo inesperado en el paquete: {qualified}')
                frame.to_sql(name, conn, schema=schema, index=False, if_exists='append', chunksize=10000)
                loaded = conn.scalar(text(f'SELECT count(*) FROM {schema}.{name}'))
                if loaded != meta['rows']:
                    raise ValueError(f'Carga incompleta: {qualified}')
    finally:
        pg.dispose()
    for path in present:
        path.unlink()
    (paths.data / '.demo-version').unlink(missing_ok=True)
    for relative in manifest['files']:
        out = paths.home / relative
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / 'files' / relative, out)
    if ensure_target_database():
        _log(f'SQL Server: base {dw_database()} creada.')
    _log(f'Fuentes reconstruidas desde {source}: {len(manifest["tables"])} tablas y {len(manifest["files"])} archivos.')


def contract_report() -> pd.DataFrame:
    """Contrato requerido de entrada (útil para preparar datos reales)."""
    rows = []
    for name, cols in CONTRACT.items():
        spec = FILE_SOURCES.get(name, {'tipo_fuente': 'POSTGRESQL', 'ubicacion': f'{RAW_SCHEMA}.{name}'})
        rows.append({'fuente': name, 'tipo': spec['tipo_fuente'], 'ubicacion': spec['ubicacion'],
                     'clave': ', '.join(KEYS[name]), 'columnas': ', '.join(cols)})
    return pd.DataFrame(rows)
