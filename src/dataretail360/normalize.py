"""Funciones de normalización: reglas explícitas y reutilizables, no correcciones silenciosas."""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime

import numpy as np
import pandas as pd


def norm(value) -> str:
    if pd.isna(value):
        return ''
    return ''.join(c for c in unicodedata.normalize('NFKD', str(value).strip().upper())
                   if not unicodedata.combining(c))


def norm_series(s: pd.Series) -> pd.Series:
    lookup = {v: norm(v) for v in s.dropna().unique()}
    return s.map(lookup).fillna('')


def code_series(s: pd.Series) -> pd.Series:
    return norm_series(s).str.replace(r'[^A-Z0-9]', '', regex=True)


def rut_valid(value):
    """RUT chileno con dígito verificador válido, normalizado como `digitos-DV`; None si no."""
    x = re.sub(r'[^0-9K]', '', norm(value))
    if len(x) < 2:
        return None
    digits, dv = x[:-1], x[-1]
    if not digits.isdigit() or int(digits) == 0:
        return None
    total = sum(int(n) * (2 + i % 6) for i, n in enumerate(reversed(digits)))
    check = 11 - total % 11
    return digits + '-' + dv if dv == ('0' if check == 11 else 'K' if check == 10 else str(check)) else None


def rut_series(s: pd.Series) -> pd.Series:
    return s.map({v: rut_valid(v) for v in s.dropna().unique()})


def region_series(s: pd.Series) -> pd.Series:
    r = norm_series(s)
    r = r.str.replace(r'^REGION (DE |DEL |DE LA |DE LOS )?', '', regex=True)
    return r.replace({'RM': 'METROPOLITANA', 'METROPOLITANA DE SANTIAGO': 'METROPOLITANA',
                      'BIO BIO': 'BIOBIO', 'BIO-BIO': 'BIOBIO', 'DEL BIOBIO': 'BIOBIO'})


def legacy_date(value):
    """Fechas legacy: no se adivinan fechas ambiguas DD/MM vs MM/DD."""
    if pd.isna(value):
        return pd.NaT
    # El XLSX contiene fechas nativas, texto y seriales de Excel. El lector textual
    # representa las fechas nativas como ISO con hora; ese formato no es ambiguo.
    if isinstance(value, (datetime, pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).normalize()
    s = str(value).strip().lower()
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?', s):
        parsed = pd.to_datetime(s, errors='coerce')
        return parsed.normalize() if pd.notna(parsed) else pd.NaT
    if re.fullmatch(r'\d{5}(?:\.0+)?', s):
        return pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(float(s)))
    months = {'ene': 'jan', 'abr': 'apr', 'ago': 'aug', 'dic': 'dec'}
    for es, en in months.items():
        s = s.replace(es, en)
    if re.fullmatch(r'\d{1,2}/\d{1,2}/\d{4}', s):
        a, b, _ = map(int, s.split('/'))
        if a <= 12 and b <= 12 and a != b:
            return pd.NaT
        return pd.to_datetime(s, format='%d/%m/%Y' if a > 12 or a == b else '%m/%d/%Y', errors='coerce')
    for fmt in ['%Y-%m-%d', '%d-%m-%y', '%d-%b-%Y']:
        try:
            return pd.Timestamp(datetime.strptime(s, fmt))
        except ValueError:
            pass
    return pd.NaT
