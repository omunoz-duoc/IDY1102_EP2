"""Adaptadores de archivo de Bronze: SHA256, copia inmutable y rechazo de entradas rotas."""
import hashlib
import io

import pandas as pd
import pytest

from dataretail360.bronze import read_file_source, validate_columns
from dataretail360.context import RunContext
from dataretail360.contracts import CONTRACT, FILE_SOURCES
from dataretail360.settings import Paths


@pytest.fixture
def ctx(tmp_path):
    context = RunContext.create(Paths(home=tmp_path, dbt_project=tmp_path / 'dbt'),
                                run_id='TEST', stream=io.StringIO(), heartbeat_seconds=0)
    yield context
    context.close()


def _write(ctx, name, frame):
    spec = FILE_SOURCES[name]
    path = ctx.paths.home / spec['ubicacion']
    path.parent.mkdir(parents=True, exist_ok=True)
    if spec['tipo_fuente'] == 'CSV':
        frame.to_csv(path, index=False, encoding='utf-8-sig')
    else:
        frame.to_excel(path, index=False, sheet_name=spec['hoja_entrada'])
    return path


def _frame(name):
    return pd.DataFrame([{col: f'{col}-1' for col in CONTRACT[name]}])


@pytest.mark.parametrize('name', list(FILE_SOURCES))
def test_file_is_frozen_by_sha_and_read_as_text(ctx, name):
    path = _write(ctx, name, _frame(name))
    frame, meta = read_file_source(ctx, name, FILE_SOURCES[name])
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert meta['sha256_archivo'] == digest
    copy = ctx.paths.home / meta['original_bronze']
    assert copy.read_bytes() == path.read_bytes()
    assert list(frame.columns) == CONTRACT[name]
    assert all(str(t) == 'string' for t in frame.dtypes)
    # Segunda lectura: reutiliza la copia y verifica su integridad.
    assert read_file_source(ctx, name, FILE_SOURCES[name])[1] == meta


def test_tampered_bronze_copy_is_rejected(ctx):
    _write(ctx, 'mkt_campanias', _frame('mkt_campanias'))
    _, meta = read_file_source(ctx, 'mkt_campanias', FILE_SOURCES['mkt_campanias'])
    (ctx.paths.home / meta['original_bronze']).write_text('alterado')
    with pytest.raises(ValueError, match='alterada'):
        read_file_source(ctx, 'mkt_campanias', FILE_SOURCES['mkt_campanias'])


def test_missing_file_has_no_fallback(ctx):
    with pytest.raises(FileNotFoundError):
        read_file_source(ctx, 'mkt_campanias', FILE_SOURCES['mkt_campanias'])


def test_missing_sheet_and_missing_column(ctx):
    spec = FILE_SOURCES['legacy_reporte_ventas_xlsx']
    path = ctx.paths.home / spec['ubicacion']
    path.parent.mkdir(parents=True)
    _frame('legacy_reporte_ventas_xlsx').to_excel(path, index=False, sheet_name='otra')
    with pytest.raises(ValueError, match='Hoja requerida'):
        read_file_source(ctx, 'legacy_reporte_ventas_xlsx', spec)
    with pytest.raises(ValueError, match='Contrato roto'):
        validate_columns('mkt_campanias', _frame('mkt_campanias').drop(columns='id_campania'))
