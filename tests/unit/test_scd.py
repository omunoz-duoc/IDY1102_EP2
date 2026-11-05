"""SCD 1/2/3, búsqueda temporal y demostraciones controladas."""
import pandas as pd
import pytest

from dataretail360.dimensions import reclassification_demo, scd_selftest
from dataretail360.scd import BASELINE, END, add_unknown, scd_merge, temporal_lookup, validate_scd

NOW = pd.Timestamp('2026-06-01')


def test_controlled_scd_case():
    d2 = scd_selftest()
    assert d2.sk.tolist() == [1, 2]


def test_first_load_is_baseline_without_history():
    inc = pd.DataFrame({'nk': ['B', 'A'], 'attr': ['x', 'y']})
    first = scd_merge(inc, pd.DataFrame(), 'nk', 'sk', type2=['attr'], now=NOW)
    assert first.nk.tolist() == ['A', 'B'] and first.sk.tolist() == [1, 2]
    assert first.valido_desde.eq(BASELINE).all() and first.valido_hasta.eq(END).all()
    assert not first.historia_observada.any()


def test_change_without_time_advance_is_rejected():
    inc = pd.DataFrame({'nk': ['A'], 'attr': ['x']})
    first = scd_merge(inc, pd.DataFrame(), 'nk', 'sk', type2=['attr'], now=NOW)
    second = scd_merge(inc.assign(attr='y'), first, 'nk', 'sk', type2=['attr'], now=NOW)
    with pytest.raises(ValueError, match='avance temporal'):
        scd_merge(inc.assign(attr='z'), second, 'nk', 'sk', type2=['attr'], now=NOW)


def test_duplicate_natural_key_is_rejected():
    with pytest.raises(ValueError):
        scd_merge(pd.DataFrame({'nk': ['A', 'A']}), pd.DataFrame(), 'nk', 'sk', now=NOW)


def test_unknown_member_and_lookup_outside_validity():
    dim = add_unknown(scd_merge(pd.DataFrame({'nk': ['A'], 'attr': ['x']}), pd.DataFrame(), 'nk', 'sk', now=NOW),
                      'nk', 'sk')
    assert dim.sk.tolist() == [0, 1] and dim.loc[0, 'attr'] == 'DESCONOCIDO'
    validate_scd(dim, 'nk', 'sk')
    events = pd.DataFrame({'nk': ['A', None, 'Z'], 'fecha': pd.to_datetime(['2026-01-01'] * 3)})
    assert list(temporal_lookup(events, events.nk, events.fecha, dim, 'nk', 'sk')) == [1, 0, 0]


def test_reclassification_keeps_previous_branch():
    stores = pd.DataFrame([{'sucursal_key': 0, 'sucursal_nk': 'DESCONOCIDO', 'pais': 'DESCONOCIDO',
                            'region': 'DESCONOCIDO', 'comuna': 'DESCONOCIDO'}])
    versions = reclassification_demo(stores)
    assert versions.categoria.tolist() == ['COCINA', 'ORGANIZACION']
