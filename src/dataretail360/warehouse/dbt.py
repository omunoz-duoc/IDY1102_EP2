"""Orquestación dbt: instantánea candidata, quality gate y evidencias por corrida.

Python conserva ingestión/SCD y restricciones finales; dbt es dueño de los SELECT de los marts.
`dbt build` confirma cada modelo por separado, sin transacción global: por eso cada corrida
usa esquemas candidatos propios y solo después de aprobar todo se promueve en una transacción.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from .. import observability, snowflake
from ..observability import CommandFailed, event, insert_progress, pipeline_step, run_logged_command
from ..settings import dbt_executable, dw_database
from .schema import safe_frame

MIN_DBT_TESTS = 33
EXPECTED_MODELS = 5
CANDIDATE_PATTERN = r'(stage|dbt|dbt_probe)_[a-f0-9]{16}'

# bcp (copia masiva del cliente SQL Server) reemplaza to_sql en staging: ~5x más rápido.
# Si no está instalado o un valor no tiene representación textual exacta, se usa to_sql.
BCP_UNSAFE = r'[\t\n\r\x00\x07]'


def bcp_text(frame):
    """Texto equivalente a lo que pyodbc insertaría; None si algún valor no es representable.

    Campo vacío = NULL; un byte NUL = cadena vacía (convención de bcp en modo carácter).
    """
    out = safe_frame(frame)
    for col in out:
        s = out[col]
        if pd.api.types.is_bool_dtype(s):
            out[col] = s.map({True: '1', False: '0'})
        elif pd.api.types.is_integer_dtype(s):
            continue
        elif pd.api.types.is_float_dtype(s):
            out[col] = s = s.astype('float64')  # to_csv usa %.17g: ida y vuelta exacta del double.
            if np.isinf(s).any():
                return None
        elif pd.api.types.is_datetime64_any_dtype(s):
            valid = s.dropna()
            if (valid.dt.microsecond % 1000).any() or valid.dt.nanosecond.any():
                return None
            out[col] = s.dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        elif pd.api.types.is_string_dtype(s) and not (s.dtype == object and s.map(type).isin([bool, np.bool_]).any()):
            if s.dtype == object and not s.map(lambda v: isinstance(v, str) or v is None or v is pd.NA or v != v).all():
                return None
            s = s.astype('string')
            if s.str.contains(BCP_UNSAFE, regex=True).any():
                return None
            out[col] = s.mask(s.eq('').fillna(False), '\0')  # NULL sigue vacío
        else:
            values = []
            for v in s:
                if v is None or v is pd.NA or (isinstance(v, float) and v != v):
                    values.append('')
                elif isinstance(v, (bool, np.bool_)):
                    values.append('1' if v else '0')
                else:
                    return None
            out[col] = values
    return out


def _source_label(path: Path, home: Path) -> str:
    try:
        return str(path.relative_to(home))
    except ValueError:
        return path.name if path.parent.name != 'dataretail360' else 'dataretail360/' + path.name


class DbtRunner:
    """Estado por corrida (`runs`) equivalente a DBT_RUNS del notebook."""

    def __init__(self, ctx, cutoff_key: int):
        self.ctx = ctx
        self.log = ctx.log
        self.dw = ctx.dw
        self.project = ctx.paths.dbt_project
        self.executable = dbt_executable(ctx.paths)
        self.cutoff_key = int(cutoff_key)
        self.runs: dict[str, dict] = {}
        self.refresh_sql: dict[str, str] = {}  # SQL dbt compilado para la prueba controlada de Clientes
        self.bcp = shutil.which('bcp') if os.name == 'nt' and os.getenv('MSSQL_AUTH', 'sql') == 'windows' else None

    # -- comandos -----------------------------------------------------------------------
    def command(self, args, folder: Path, source_schema, candidate_schema, expected_failure=False):
        """Ejecuta el CLI aislado; conserva salida técnica, sin imprimir filas ni credenciales."""
        folder.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.pop('PGPASSWORD', None)  # dbt solo utiliza la conexión a SQL Server.
        env['DBT_CANDIDATE_SCHEMA'] = candidate_schema
        command = [str(self.executable), *args, '--project-dir', str(self.project), '--profiles-dir', str(self.project),
                   '--target-path', str(folder), '--log-path', str(folder / 'logs'),
                   '--vars', json.dumps({'source_schema': source_schema, 'customer_cutoff_key': self.cutoff_key}),
                   '--no-use-colors', '--no-send-anonymous-usage-stats']
        return run_logged_command(self.log, command, cwd=self.ctx.paths.home, env=env,
                                  log_path=folder / ('command_' + args[0] + '.log'),
                                  label='dbt.' + args[0] + ('.' + args[1] if args[0] == 'docs' else ''),
                                  timeout=2400, expected_failure=expected_failure)

    def gate(self, folder: Path, source_schema, candidate_schema, expected_failure=False):
        """No se promueve una ejecución parcial, con pruebas omitidas o con modelos en otra base."""
        results = json.loads((folder / 'run_results.json').read_text(encoding='utf-8'))
        graph = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
        expected = {key for key, node in graph['nodes'].items() if node['resource_type'] in ('model', 'test')}
        actual = {row['unique_id'] for row in results['results']}
        if actual != expected or any(row['status'] not in ('success', 'pass') for row in results['results']):
            bad = [row['unique_id'] for row in results['results'] if row['status'] not in ('success', 'pass')]
            event(self.log, 'dbt_gate_rejected', 'No se autoriza promover resultados incompletos o rechazados.',
                  level=logging.WARNING if expected_failure else logging.ERROR, expected=expected_failure,
                  object=', '.join(bad[:5]), artifact=str(folder / 'run_results.json'),
                  action='Revisar pruebas fallidas u omitidas; conservar la publicación anterior.')
            raise ValueError('Quality gate dbt: resultado incompleto o no aprobado')
        models = [node for node in graph['nodes'].values() if node['resource_type'] == 'model']
        if len(models) != EXPECTED_MODELS or any(node['schema'] != candidate_schema or node['database'] != dw_database()
                                                 for node in models):
            raise ValueError('dbt intentó materializar modelos fuera del destino candidato')
        if any(node['schema'] != source_schema for node in graph['sources'].values()):
            raise ValueError('La fuente dbt no corresponde a la instantánea de esta corrida')
        tests = [row for row in results['results'] if row['unique_id'].startswith('test.')]
        if len(tests) < MIN_DBT_TESTS:
            raise ValueError('Faltan controles dbt del proyecto')
        event(self.log, 'dbt_gate_passed', 'Todos los modelos y controles requeridos aprobaron.',
              tests_passed=len(tests), models=len(models))
        return graph, results

    # -- staging --------------------------------------------------------------------------
    def load_stage_table(self, name, frame, schema, metadata, temp: Path, folder: Path):
        """Carga una tabla candidata ya creada (esquema privado) y concilia su conteo."""
        data = bcp_text(frame) if self.bcp else None
        if data is not None:
            path = temp / (name + '.tsv')
            data.to_csv(path, sep='\t', header=False, index=False, lineterminator='\n', encoding='utf-8',
                        na_rep='', float_format='%.17g', quoting=csv.QUOTE_NONE, quotechar='\x07')
            env = os.environ.copy()
            env.pop('PGPASSWORD', None)
            command = [self.bcp, f'{schema}.{name}', 'in', str(path),
                       '-S', os.getenv('MSSQL_SERVER', 'localhost'), '-d', dw_database(),
                       '-T', '-u', '-c', '-C', '65001', '-t', '\\t', '-r', '0x0a', '-b', '100000', '-h', 'TABLOCK',
                       '-m', '1', '-e', str(folder / 'bcp' / (name + '.err'))]
            (folder / 'bcp').mkdir(parents=True, exist_ok=True)
            try:
                run_logged_command(self.log, command, cwd=self.ctx.paths.home, env=env,
                                   log_path=folder / 'bcp' / (name + '.log'), label='staging.bcp', timeout=1200)
            except CommandFailed:
                data = None
            else:
                with self.dw.connect() as conn:
                    if conn.scalar(text(f'SELECT COUNT_BIG(*) FROM [{schema}].[{name}]')) != len(frame):
                        data = None
            if data is None:
                event(self.log, 'bcp_fallback', 'bcp no concilió; la tabla candidata se recarga con to_sql.',
                      level=logging.WARNING, object=name)
                with self.dw.begin() as conn:
                    conn.execute(text(f'TRUNCATE TABLE [{schema}].[{name}]'))
        if data is None:
            with self.dw.begin() as conn:
                with insert_progress(self.log, conn, schema + '.' + name, len(frame)):
                    safe_frame(frame).to_sql(name, conn, schema=schema, if_exists='append', index=False,
                                             chunksize=10000,
                                             dtype={col: metadata.tables['dw.' + name].c[col].type for col in frame})
        with self.dw.connect() as conn, pipeline_step(self.log, 'staging.conciliar', object=name, rows_total=len(frame)):
            if conn.scalar(text(f'SELECT COUNT_BIG(*) FROM [{schema}].[{name}]')) != len(frame):
                raise ValueError('Instantánea candidata incompleta: ' + name)

    def cleanup(self, run_id):
        """Elimina solo esquemas privados de esta corrida; nunca dw, dm_* ni dbt_dev."""
        with pipeline_step(self.log, 'publicacion.limpiar_candidatos', publication_id=str(run_id)):
            context = self.runs.get(run_id)
            if not context:
                return
            with self.dw.begin() as conn:
                for schema in [context['candidate_schema'], context['source_schema']]:
                    if not re.fullmatch(CANDIDATE_PATTERN, schema):
                        raise ValueError('Nombre no autorizado para limpieza de candidatos')
                    objects = conn.execute(text('''SELECT o.name,o.type FROM sys.objects o
                    JOIN sys.schemas s ON o.schema_id=s.schema_id
                    WHERE s.name=:s AND o.type IN ('V','U') ORDER BY CASE WHEN o.type='V' THEN 0 ELSE 1 END'''),
                                           {'s': schema}).all()
                    for name, kind in objects:
                        escaped = name.replace(']', ']]')
                        conn.execute(text(f'DROP {"VIEW" if kind.strip() == "V" else "TABLE"} [{schema}].[{escaped}]'))
                    if conn.scalar(text('SELECT SCHEMA_ID(:s)'), {'s': schema}) is not None:
                        conn.execute(text(f'DROP SCHEMA [{schema}]'))
            context['candidates_cleaned'] = True

    def prepare(self, run_id, all_frames: dict, metadata: sa.MetaData):
        """Carga una sola vez los frames candidatos; dbt y la promoción leen esa misma versión."""
        with pipeline_step(self.log, 'publicacion.preparar_candidatos', publication_id=str(run_id)):
            return self._prepare(run_id, all_frames, metadata)

    def _prepare(self, run_id, all_frames, metadata):
        home = self.ctx.paths.home
        token = hashlib.sha256(run_id.encode()).hexdigest()[:16]
        source_schema = 'stage_' + token
        candidate_schema = 'dbt_' + token
        folder = self.ctx.paths.artifacts / 'dbt' / run_id
        context = {'source_schema': source_schema, 'candidate_schema': candidate_schema,
                   'artifact_path': folder.relative_to(home).as_posix(), 'candidates_cleaned': False}
        self.runs[run_id] = context
        stage_meta = sa.MetaData()
        for name, frame in all_frames.items():
            original = metadata.tables['dw.' + name]
            sa.Table(name, stage_meta, *[sa.Column(col, original.c[col].type, primary_key=original.c[col].primary_key,
                                                   nullable=original.c[col].nullable, autoincrement=False)
                                         for col in frame], schema=source_schema)
        with self.dw.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA [{source_schema}]'))
            with pipeline_step(self.log, 'staging.crear_tablas', tables=len(all_frames)):
                stage_meta.create_all(conn)
        # Esquema privado confirmado: bcp corre en otro proceso y debe ver las tablas.
        # Ante cualquier error, cleanup elimina el esquema completo.
        with tempfile.TemporaryDirectory(prefix='dr360_bcp_') as temp:
            for name, frame in all_frames.items():
                self.load_stage_table(name, frame, source_schema, metadata, Path(temp), folder)
                self.log.info('stage run=%s tabla=%s filas=%d', run_id, name, len(frame))
        self.command(['build', '--no-partial-parse'], folder, source_schema, candidate_schema)
        graph, results = self.gate(folder, source_schema, candidate_schema)
        # docs generate vuelve a compilar: preservar los artefactos exactos del build aprobado.
        for name in ['manifest.json', 'run_results.json']:
            shutil.copy2(folder / name, folder / ('build_' + name))
        self.command(['docs', 'generate', '--static'], folder, source_schema, candidate_schema)
        files = [self.project / 'dbt_project.yml', self.project / 'profiles.yml',
                 Path(snowflake.__file__), Path(observability.__file__)]
        files += [p for p in [self.project.parent / 'requirements' / 'dbt.lock.txt',
                              self.project.parent / 'requirements-dbt-lock.txt'] if p.is_file()]
        files += [p for sub in ['models', 'tests', 'macros'] for p in (self.project / sub).rglob('*') if p.is_file()]
        context.update({
            'tests_passed': sum(r['unique_id'].startswith('test.') for r in results['results']),
            'models_built': sum(r['unique_id'].startswith('model.') for r in results['results']),
            'invocation_id': results['metadata']['invocation_id'],
            'dbt_version': results['metadata']['dbt_version'],
            'customer_cutoff_key': self.cutoff_key,
            'source_sha256': {_source_label(p, home): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in sorted(files)},
            'artifact_sha256': {name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
                                for name in ['build_manifest.json', 'build_run_results.json', 'catalog.json',
                                             'static_index.html']}})
        # La prueba ficticia de SCD/meses vacíos reutiliza el SQL compilado de producción de dbt.
        # No se conserva un segundo SELECT de Clientes en Python.
        database = dw_database()
        sql = graph['nodes']['model.dataretail360.clientes_mensuales']['compiled_code']
        for name in all_frames:
            sql = sql.replace(f'[{database}].[{source_schema}].[{name}]', 'dw.' + name)
            sql = sql.replace(f'"{database}"."{source_schema}"."{name}"', 'dw.' + name)
        if source_schema in sql:
            raise ValueError('Relación candidata sin normalizar en prueba de Clientes')
        sql = re.sub(r'\b' + str(self.cutoff_key) + r'\b', ':customer_cutoff_key', sql)
        head, separator, tail = sql.rpartition('SELECT * FROM resultado')
        if not separator or tail.strip():
            raise ValueError('SQL de Clientes no admite la prueba controlada')
        self.refresh_sql['dm_clientes'] = head + 'INSERT INTO dm_clientes.clientes_mensuales SELECT * FROM resultado'
        (folder / 'publication.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
        self.log.info('dbt run=%s modelos=%d pruebas=%d', run_id, context['models_built'], context['tests_passed'])
        return context
