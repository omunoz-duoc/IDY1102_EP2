"""Orquestación de extremo a extremo.

PostgreSQL (14) + CSV + Excel → Bronze → Silver → dimensiones SCD/snowflake → hechos →
candidatos + dbt → promoción atómica DWH + 4 marts → Gold → análisis, pruebas y reporte.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from . import bronze, dimensions as dim, facts as fct, gold, silver as slv
from .context import RunContext
from .observability import pipeline_step
from .quality import QualityAuditor
from .settings import dw_database


@dataclass
class PipelineState:
    """Resultados intermedios disponibles para análisis y pruebas posteriores."""
    bronze: Any = None
    auditor: Any = None
    silver: Any = None
    dims: Any = None
    sf_versions: Any = None
    facts: Any = None
    warehouse: Any = None
    mart_frames: dict = field(default_factory=dict)
    mart_catalog: Any = None
    dbt: dict = field(default_factory=dict)
    checks: list = field(default_factory=list)

    @property
    def cutoff_key(self) -> int:
        return int(self.bronze.customer_cutoff.strftime('%Y%m%d'))


def check_connections(ctx: RunContext) -> None:
    # TrustServerCertificate es una decisión del entorno local, no de producción.
    with pipeline_step(ctx.log, 'configuracion.conexion_sqlserver'):
        with ctx.dw.connect() as conn:
            current = conn.scalar(text('SELECT DB_NAME()'))
        if current != dw_database():
            raise RuntimeError(f'Base destino inesperada: {current}')
    ctx.log.info('Conexiones configuradas; SQL Server disponible. Run: %s', ctx.run_id)


def build(ctx: RunContext) -> PipelineState:
    """Bronze → Silver → dimensiones → hechos (sin escribir en el DWH)."""
    from .dimensions import WarehouseHistory
    state = PipelineState()
    state.bronze = bronze.extract(ctx)
    state.auditor = QualityAuditor(ctx.log, ctx.paths.rejects)
    with pipeline_step(ctx.log, 'silver.integrar'):
        state.silver = slv.build_silver(state.bronze.raw, state.auditor, ctx.paths.silver, ctx.paths.control,
                                        ctx.report)
    state.dims = dim.build_dimensions(state.silver, WarehouseHistory(ctx.dw), log=ctx.log)
    with pipeline_step(ctx.log, 'pruebas.scd', heartbeat=False):
        scd_demo = dim.scd_selftest()
    with pipeline_step(ctx.log, 'pruebas.reclasificacion', heartbeat=False):
        state.sf_versions = dim.reclassification_demo(state.dims.logical['dim_sucursal'])
    dim.report_dimensions(ctx.report, state.dims, scd_demo, state.sf_versions)
    state.facts = fct.build_facts(state.silver, state.dims, state.auditor, ctx.log, ctx.paths.control, ctx.report)
    return state


def publish(ctx: RunContext, state: PipelineState) -> PipelineState:
    """Candidatos + dbt + promoción atómica, y exportaciones Gold de lo publicado."""
    from .warehouse.publish import Warehouse
    with pipeline_step(ctx.log, 'dw.preparar_y_publicar', heartbeat=False):
        state.warehouse = Warehouse(ctx, state.dims, state.facts.tables, state.cutoff_key)
        state.warehouse.prepare()
        state.warehouse.publish(ctx.run_id)
        ctx.log.info('Publicación SQL Server completada: %d filas de hechos y 4 data marts.',
                     sum(len(f) for f in state.facts.tables.values()))
    gold.export_warehouse(ctx, state.warehouse)
    state.mart_frames, state.mart_catalog = gold.export_marts(ctx, state.warehouse)
    state.dbt = gold.dbt_evidence(ctx, state.warehouse, state.mart_frames)
    return state


def run(ctx: RunContext, verify: bool = True) -> PipelineState:
    """Corrida completa. `verify=False` omite la segunda publicación y las sondas."""
    from .analysis import run_analysis
    from .closing import benchmark, conclusions
    from .dictionary import write_dictionaries
    from .verification import Verifier
    check_connections(ctx)
    state = build(ctx)
    state = publish(ctx, state)
    analysis = run_analysis(ctx, state)
    write_dictionaries(ctx, state.warehouse.all_frames, state.mart_frames)
    if verify:
        state.checks = Verifier(ctx, state, analysis).run()
        benchmark(ctx, state)
    else:
        state.checks = list(state.bronze.checks)
    conclusions(ctx, state, analysis, state.checks)
    return state
