"""Reglas monetarias de los hechos: prorrateo entero y neto sin IVA."""
import pandas as pd

from dataretail360.facts import allocate_integer, date_key, money_round


def test_largest_remainder_keeps_document_total():
    mini = pd.DataFrame({'doc': ['A'] * 3, 'peso': [1, 1, 1], 'neto': [10] * 3})
    assert list(allocate_integer(mini, 'doc', 'peso', 'neto')) == [4, 3, 3]
    weighted = pd.DataFrame({'doc': ['A', 'A', 'B'], 'peso': [1190, 2380, 500], 'neto': [3000, 3000, 420]})
    result = allocate_integer(weighted, 'doc', 'peso', 'neto')
    assert result.tolist() == [1000, 2000, 420]


def test_money_round_and_date_key():
    assert money_round(pd.Series([1190, 119, 60])).tolist() == [1000, 100, 50]
    assert date_key(pd.Series(pd.to_datetime(['2026-09-30 23:59']))).tolist() == [20260930]
