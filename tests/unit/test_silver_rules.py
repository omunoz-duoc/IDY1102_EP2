"""Reglas de normalización y balance de calidad (sin bases de datos)."""
import io
from datetime import datetime

import pandas as pd

from dataretail360.normalize import code_series, legacy_date, norm, region_series, rut_valid
from dataretail360.observability import configure_logging
from dataretail360.quality import QualityAuditor


def test_norm_removes_accents_and_case():
    assert norm('  Ñuñoa ') == 'NUNOA'
    assert norm(None) == ''
    assert code_series(pd.Series(['p-1', 'P 2', None])).tolist() == ['P1', 'P2', '']


def test_rut_requires_valid_check_digit():
    assert rut_valid('11.111.111-1') == '11111111-1'
    assert rut_valid('11111111-2') is None
    assert rut_valid('0-0') is None
    assert rut_valid('') is None


def test_region_homologation():
    regions = pd.Series(['RM', 'Región del Biobío', 'Bio-Bio', 'Región de Valparaíso'])
    assert region_series(regions).tolist() == ['METROPOLITANA', 'BIOBIO', 'BIOBIO', 'VALPARAISO']


def test_legacy_dates_never_guess_ambiguous_formats():
    assert legacy_date(datetime(2023, 1, 13)) == pd.Timestamp('2023-01-13')
    assert legacy_date('2023-01-13 00:00:00') == pd.Timestamp('2023-01-13')
    assert legacy_date('44999') == pd.Timestamp('1899-12-30') + pd.Timedelta(days=44999)
    assert pd.isna(legacy_date('01/02/2023'))
    assert pd.isna(legacy_date('2023-99-01 00:00:00'))
    assert legacy_date('13/01/2023') == pd.Timestamp('2023-01-13')
    assert legacy_date('05/05/2023') == pd.Timestamp('2023-05-05')


def test_auditor_counts_each_row_once(tmp_path):
    log = configure_logging(tmp_path, 'Q', stream=io.StringIO(), heartbeat_seconds=0)
    auditor = QualityAuditor(log, tmp_path)
    df = pd.DataFrame({'k': ['a', 'a', 'b', 'b', None], 'v': [1, 1, 2, 3, 4],
                       '_src_row': range(5), '_batch': 'x'})
    kept = auditor.audit(df, 't', {'clave_nula': df.k.isna(), 'negativo': df.v.lt(0)}, ['k'])
    row = auditor.quality[0]
    # a duplicado exacto (1), b clave conflictiva (2), None clave nula (1); se conserva una 'a'.
    assert (row['leidas'], row['aceptadas'], row['rechazadas'], row['duplicadas']) == (5, 1, 3, 1)
    assert kept.k.tolist() == ['a']
    assert set(auditor.rejections_frame()._motivo) == {'clave_nula;', 'clave_conflictiva'}
    assert (tmp_path / 't.parquet').exists() and (tmp_path / 't_duplicados.parquet').exists()
    for handler in log.handlers[:]:
        log.removeHandler(handler)
        handler.close()
