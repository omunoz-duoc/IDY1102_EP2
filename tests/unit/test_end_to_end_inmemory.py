"""Cadena completa Silver → dimensiones → hechos con el dataset sintético, sin bases de datos.

Usa los mismos lectores textuales de CSV/Excel que Bronze. Las cifras esperadas son las de la
ejecución verificada del notebook original sobre el mismo dataset.
"""
import io

import numpy as np
import pandas as pd
import pytest

from dataretail360.bronze import read_file_source
from dataretail360.context import RunContext
from dataretail360.contracts import CONTRACT, FILE_SOURCES
from dataretail360.dimensions import build_dimensions
from dataretail360.facts import build_facts
from dataretail360.quality import QualityAuditor
from dataretail360.seed import synthetic
from dataretail360.settings import Paths
from dataretail360.silver import build_silver
from dataretail360.warehouse.schema import validate_destination


@pytest.fixture(scope='module')
def pipeline(tmp_path_factory):
    home = tmp_path_factory.mktemp('home')
    ctx = RunContext.create(Paths(home=home, dbt_project=home / 'dbt'), run_id='E2E', stream=io.StringIO(),
                            heartbeat_seconds=0)
    raw = synthetic.frames()
    synthetic.write_files(home, raw)
    for name, spec in FILE_SOURCES.items():
        raw[name] = read_file_source(ctx, name, spec)[0]
    for frame in raw.values():
        frame['_src_row'] = np.arange(len(frame), dtype='int64')
        frame['_batch'] = 'demo'
    auditor = QualityAuditor(ctx.log, ctx.paths.rejects)
    silver = build_silver(raw, auditor, ctx.paths.silver, ctx.paths.control)
    dims = build_dimensions(silver, observed_at=pd.Timestamp('2026-10-01'), log=ctx.log)
    facts = build_facts(silver, dims, auditor, ctx.log, ctx.paths.control)
    yield silver, dims, facts, auditor
    ctx.close()


def test_synthetic_sources_are_deterministic_and_complete():
    first, second = synthetic.frames(), synthetic.frames()
    assert set(first) == set(CONTRACT)
    for name, frame in first.items():
        assert not frame.empty and list(frame.columns) == CONTRACT[name]
        pd.testing.assert_frame_equal(frame, second[name])


def test_fact_counts_and_amounts(pipeline):
    _, _, facts, _ = pipeline
    t = facts.tables
    assert {n: len(f) for n, f in t.items()} == {'fact_ventas': 64, 'fact_devoluciones': 5, 'fact_inventario': 36,
                                                 'fact_marketing': 32, 'fact_web': 32, 'fact_reposicion': 1}
    assert int(t['fact_ventas'].monto_neto.sum()) == 192000
    assert int(t['fact_devoluciones'].monto_neto_devuelto.sum()) == 6000
    assert t['fact_ventas'].cliente_key.eq(0).any()          # compras anónimas
    assert t['fact_devoluciones'].fecha_key.gt(20260930).any()  # reembolso posterior al corte
    assert int(t['fact_marketing'].sin_consentimiento.sum()) == 4


def test_dimensions_and_destination_contract(pipeline):
    _, dims, facts, _ = pipeline
    assert len(dims.physical) == 12
    assert {n: len(dims.logical[n]) for n in ['dim_producto', 'dim_cliente', 'dim_sucursal']} == \
        {'dim_producto': 7, 'dim_cliente': 9, 'dim_sucursal': 5}
    validate_destination(facts.tables, dims.logical)


def test_quality_balance_and_quarantine(pipeline):
    _, _, _, auditor = pipeline
    q = auditor.quality_frame()
    assert q.leidas.eq(q.aceptadas + q.rechazadas + q.duplicadas).all()
    legacy = q.set_index('etapa').loc['legacy_formatos']
    assert (legacy.leidas, legacy.rechazadas) == (2, 1)  # fecha ambigua inyectada
