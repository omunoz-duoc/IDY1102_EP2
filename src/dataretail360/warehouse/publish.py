"""Publicación del DWH y de los cuatro data marts (Write–Audit–Publish).

1. Se validan los hechos y se carga una instantánea candidata en un esquema propio.
2. `dbt build` materializa cuatro resúmenes y una vista intermedia y exige aprobar sus pruebas.
3. Una sola transacción SQL publica el DWH, sus agregados y los cuatro marts, con bloqueo de
   aplicación. Un fallo antes del commit conserva la versión anterior de todo el conjunto.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from ..observability import event, pipeline_step
from ..snowflake import DIMENSION_LINKS, REMOVED_ATTRIBUTES
from .dbt import DbtRunner
from .marts import clear_datamarts, ensure_datamarts, refresh_datamarts
from .schema import GOLD_MONTHLY_DDL, build_metadata, validate_destination


def migrate_snowflake(conn, log):
    """Migración reversible por transacción: se ejecuta con las dimensiones vacías y padres primero."""
    with pipeline_step(log, 'dw.migracion_snowflake'):
        for table, attributes in REMOVED_ATTRIBUTES.items():
            key = next(iter(DIMENSION_LINKS[table]))
            exists = conn.scalar(text('SELECT COL_LENGTH(:t,:c)'), {'t': 'dw.' + table, 'c': key})
            if exists is None:
                conn.execute(text(f'ALTER TABLE dw.{table} ADD [{key}] bigint NOT NULL'))
            for column in attributes:
                if conn.scalar(text('SELECT COL_LENGTH(:t,:c)'), {'t': 'dw.' + table, 'c': column}) is not None:
                    conn.execute(text(f'ALTER TABLE dw.{table} DROP COLUMN [{column}]'))
        for table, links in DIMENSION_LINKS.items():
            for key, parent in links.items():
                exists = conn.scalar(text('''SELECT COUNT(*) FROM sys.foreign_key_columns
                WHERE parent_object_id=OBJECT_ID(:t) AND parent_column_id=COLUMNPROPERTY(OBJECT_ID(:t),:c,'ColumnId')'''),
                                     {'t': 'dw.' + table, 'c': key})
                if not exists:
                    conn.execute(text(f'ALTER TABLE dw.{table} WITH CHECK ADD CONSTRAINT fk_sf_{table}_{key} '
                                      f'FOREIGN KEY([{key}]) REFERENCES dw.{parent}([{key}])'))


class Warehouse:
    """Destino SQL Server: contratos físicos, candidatos dbt y promoción atómica."""

    def __init__(self, ctx, dims, facts, cutoff_key: int):
        self.ctx = ctx
        self.log = ctx.log
        self.dw = ctx.dw
        self.dimensions = dims.logical
        self.physical = dims.physical
        self.facts = facts
        self.all_frames = {**self.physical, **self.facts}
        self.cutoff_key = int(cutoff_key)
        self.dbt = DbtRunner(ctx, self.cutoff_key)
        self.metadata = None

    @property
    def runs(self):
        return self.dbt.runs

    def validate(self, frames=None):
        validate_destination(frames if frames is not None else self.facts, self.dimensions)

    def prepare(self):
        """Valida hechos y crea esquemas/tablas finales si no existen (sin to_sql replace)."""
        self.validate()
        with self.dw.begin() as conn:
            for schema in ['dw', 'ctl']:
                conn.execute(text(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA {schema}')"))
        self.metadata = build_metadata(self.physical, self.facts, self.log)
        self.metadata.create_all(self.dw)
        with self.dw.begin() as conn:
            conn.execute(text(GOLD_MONTHLY_DDL))

    def publish(self, run_id):
        """Recarga completa transaccional; las dimensiones incluyen todas sus versiones previas."""
        with pipeline_step(self.log, 'publicacion.completa', publication_id=str(run_id)):
            self._publish(run_id)

    def _publish(self, run_id):
        if self.metadata is None:
            self.prepare()
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.dw.begin() as conn:
            conn.execute(text('INSERT INTO ctl.ejecuciones(run_id,inicio_utc,estado) VALUES(:id,:ts,\'RUNNING\')'),
                         {'id': run_id, 'ts': started})
        try:
            with pipeline_step(self.log, 'publicacion.validar_hechos'):
                self.validate()
            staged = self.dbt.prepare(run_id, self.all_frames, self.metadata)  # Sin tocar la publicación final.
            with pipeline_step(self.log, 'publicacion.transaccion_sql', publication_id=run_id):
                with self.dw.begin() as conn:
                    conn.execute(text('SET XACT_ABORT ON'))
                    # Bloqueo de aplicación: impide dos publicaciones concurrentes.
                    with pipeline_step(self.log, 'publicacion.esperar_bloqueo',
                                       action='Esperar hasta 10 segundos por el bloqueo de publicación.'):
                        lock = conn.execute(text("DECLARE @r int; EXEC @r=sp_getapplock @Resource='dr360_publish',"
                                                 "@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000; "
                                                 "SELECT @r;")).scalar()
                    if lock is not None and lock < 0:
                        raise RuntimeError('No se obtuvo bloqueo de publicación')
                    # Tablas/vistas y corte de cobertura cambian dentro de la misma promoción.
                    clear_datamarts(conn)
                    for name in self.facts:
                        with pipeline_step(self.log, 'dw.eliminar_hechos', object=name):
                            conn.execute(text(f'DELETE FROM dw.{name}'))
                    for name in reversed(self.physical):
                        with pipeline_step(self.log, 'dw.eliminar_dimension', object=name):
                            conn.execute(text(f'DELETE FROM dw.{name}'))
                    migrate_snowflake(conn, self.log)
                    ensure_datamarts(conn, self.log, self.cutoff_key)
                    for name, frame in self.all_frames.items():
                        identity = conn.scalar(text("SELECT COUNT(*) FROM sys.identity_columns WHERE object_id=OBJECT_ID(:name)"),
                                               {'name': 'dw.' + name})
                        if identity:
                            conn.execute(text(f'SET IDENTITY_INSERT dw.{name} ON'))
                        # Misma instantánea que leyó dbt; no se vuelve a extraer ni a recalcular SCD.
                        cols = ','.join('[' + col + ']' for col in frame.columns)
                        conn.execute(text(f'INSERT INTO dw.{name} ({cols}) SELECT {cols} FROM [{staged["source_schema"]}].[{name}]'))
                        if identity:
                            conn.execute(text(f'SET IDENTITY_INSERT dw.{name} OFF'))
                        count = conn.scalar(text(f'SELECT COUNT_BIG(*) FROM dw.{name}'))
                        if count != len(frame):
                            raise ValueError(f'Conteo no concilia: {name}')
                        self.log.info('load tabla=%s filas=%d', name, count)
                    loaded_sales = conn.scalar(text('SELECT SUM(monto_neto) FROM dw.fact_ventas'))
                    if int(loaded_sales) != int(self.facts['fact_ventas'].monto_neto.sum()):
                        raise ValueError('Importe destino no concilia')
                    conn.execute(text('DELETE FROM dw.gold_ventas_mes'))
                    conn.execute(text('''INSERT INTO dw.gold_ventas_mes
            SELECT d.anio,d.mes,f.canal_key,SUM(f.monto_neto),COUNT_BIG(DISTINCT f.documento_nk),SUM(f.cantidad)
            FROM dw.fact_ventas f JOIN dw.dim_fecha d ON f.fecha_key=d.fecha_key
            GROUP BY d.anio,d.mes,f.canal_key'''))
                    # Promoción de cuatro candidatos dbt: conciliación adicional antes del commit global.
                    refresh_datamarts(conn, run_id, staged['candidate_schema'], self.log, self.cutoff_key)
                    conn.execute(text("UPDATE ctl.ejecuciones SET estado='OK',fin_utc=:ts,tablas=:n,filas_hechos=:f WHERE run_id=:id"),
                                 {'ts': datetime.now(timezone.utc).replace(tzinfo=None), 'n': len(self.all_frames),
                                  'f': sum(len(f) for f in self.facts.values()), 'id': run_id})
            staged['publication_status'] = 'OK'
            event(self.log, 'publication_committed', 'COMMIT confirmado: DWH y cuatro marts publicados.',
                  publication_id=run_id, rows=sum(len(f) for f in self.facts.values()), tables=len(self.all_frames),
                  action='La exportación Gold y las pruebas restantes continúan después de este commit.')
        except BaseException as exc:
            status = 'CANCELLED' if isinstance(exc, (KeyboardInterrupt, SystemExit)) else 'FAILED'
            if run_id in self.runs:
                self.runs[run_id]['publication_status'] = status
            event(self.log, 'publication_not_committed', 'La publicación no se confirmó; conservar la versión anterior.',
                  level=logging.WARNING if status == 'CANCELLED' else logging.ERROR, publication_id=run_id,
                  error_type=type(exc).__name__,
                  action='Revisar la etapa fallida y sus artefactos antes de volver a ejecutar.')
            try:
                with self.dw.begin() as conn:
                    conn.execute(text("UPDATE ctl.ejecuciones SET estado=:status,fin_utc=:ts WHERE run_id=:id"),
                                 {'ts': datetime.now(timezone.utc).replace(tzinfo=None), 'id': run_id, 'status': status})
            except Exception as audit_error:
                event(self.log, 'audit_update_failed', 'No se pudo registrar el estado final; se conserva la excepción original.',
                      level=logging.ERROR, publication_id=run_id, error_type=type(audit_error).__name__,
                      action='Revisar ctl.ejecuciones y la conexión SQL; no asumir éxito por una fila RUNNING.')
            raise
        finally:
            # Un error de limpieza no modifica un commit ya válido; queda visible en el log.
            try:
                self.dbt.cleanup(run_id)
            except Exception as cleanup_error:
                event(self.log, 'cleanup_pending', 'Quedaron candidatos pendientes de limpieza; no cambia un commit ya confirmado.',
                      level=logging.WARNING, publication_id=run_id, error_type=type(cleanup_error).__name__,
                      action='Revisar publication.json y limpiar únicamente los esquemas candidatos de esta corrida.')
            context = self.runs.get(run_id)
            if context:
                folder = self.ctx.paths.home / context['artifact_path']
                folder.mkdir(parents=True, exist_ok=True)
                try:
                    (folder / 'publication.json').write_text(json.dumps(context, ensure_ascii=False, indent=2),
                                                            encoding='utf-8')
                except Exception as audit_error:
                    event(self.log, 'audit_file_failed', 'No se pudo guardar la evidencia de publicación en disco.',
                          level=logging.WARNING, publication_id=run_id, error_type=type(audit_error).__name__,
                          action='Comprobar permisos y espacio; consultar el evento publication_committed y ctl.ejecuciones.')
