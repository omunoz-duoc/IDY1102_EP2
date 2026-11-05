"""Contexto de una corrida: identificador, rutas, logger, conexiones, manifiesto y reporte.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from .observability import configure_logging
from .report import Report
from .settings import Paths, postgres_engine, secrets_from_env, sqlserver_engine


@dataclass
class RunContext:
    paths: Paths
    run_id: str
    log: logging.Logger
    report: Report
    manifest: dict[str, Any] = field(default_factory=dict)
    _pg: Any = None
    _dw: Any = None

    @classmethod
    def create(cls, paths: Paths | None = None, run_id: str | None = None, stream=None,
               heartbeat_seconds: float = 30, slow_seconds: float = 300) -> 'RunContext':
        paths = paths or Paths.from_env()
        paths.ensure()
        run_id = run_id or uuid.uuid4().hex
        # Los mensajes de avance no imprimen filas, identificadores de personas ni parámetros SQL.
        log = configure_logging(paths.home, run_id, stream=stream, secrets=secrets_from_env(),
                                heartbeat_seconds=heartbeat_seconds, slow_seconds=slow_seconds)
        return cls(paths=paths, run_id=run_id, log=log,
                   report=Report('DataRetail 360 · Reporte de ejecución'))

    @property
    def pg(self):
        if self._pg is None:
            self._pg = postgres_engine()
        return self._pg

    @property
    def dw(self):
        if self._dw is None:
            self._dw = sqlserver_engine()
        return self._dw

    @property
    def manifest_path(self):
        return self.paths.control / f'{self.run_id}.json'

    def write_manifest(self) -> None:
        self.manifest_path.write_text(json.dumps(self.manifest, ensure_ascii=False, indent=2,
                                                 default=str), encoding='utf-8')

    @property
    def report_path(self):
        return self.paths.artifacts / 'reports' / self.run_id / 'report.md'

    def close(self) -> None:
        for engine in (self._pg, self._dw):
            if engine is not None:
                engine.dispose()
        for handler in self.log.handlers[:]:
            self.log.removeHandler(handler)
            handler.close()
