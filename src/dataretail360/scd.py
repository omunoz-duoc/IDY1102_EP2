"""Historia temporal de dimensiones: SCD tipos 1, 2 y 3 con intervalos [desde, hasta).

Política: la primera observación es una línea base (1900-01-01), no historia reconstruida.
Desde ejecuciones futuras, los cambios de atributos generan intervalos [desde, hasta).
Funciones puras: no leen ni escriben bases de datos.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

BASELINE = pd.Timestamp('1900-01-01')
END = pd.Timestamp('2100-01-01')


def observed_now() -> pd.Timestamp:
    """Momento de observación en hora local de Chile, sin zona y truncado a segundos."""
    return pd.Timestamp.now(tz='America/Santiago').tz_localize(None).floor('s')


def scd_merge(incoming, previous, nk, sk, type2=(), type1=(), type3=(), now=None):
    """Conserva claves y tipos 1/2/3. No crea versiones si los atributos no cambian."""
    if now is None:
        raise ValueError('scd_merge requiere el instante de observación `now`')
    inc = incoming.copy().reset_index(drop=True)
    if inc[nk].duplicated().any() or inc[nk].isna().any():
        raise ValueError('Clave natural no única')
    old = previous.copy()
    attrs = list(inc.columns.drop(nk))
    if old.empty:
        result = inc.sort_values(nk).reset_index(drop=True)
        result.insert(0, sk, np.arange(1, len(result) + 1, dtype='int64'))
        for col in type3:
            result[col + '_anterior'] = 'SIN_HISTORIA'
        result['valido_desde'] = BASELINE
        result['valido_hasta'] = END
        result['es_actual'] = True
        result['historia_observada'] = False
        return result
    for col in ['valido_desde', 'valido_hasta']:
        old[col] = pd.to_datetime(old[col])
    current = old.loc[old.es_actual.astype(bool)]
    if current[nk].duplicated().any():
        raise ValueError('Más de una versión vigente')
    merged = inc.merge(current, on=nk, how='left', suffixes=('', '_old'), validate='one_to_one')
    new = merged[sk].isna()
    changed = pd.Series(False, index=merged.index)
    for col in type2:
        changed |= merged[col].fillna('').astype(str).ne(merged[col + '_old'].fillna('').astype(str))
    changed &= ~new
    for col in type1:
        updates = inc.set_index(nk)[col]
        mask = old[nk].isin(updates.index)
        old.loc[mask, col] = old.loc[mask, nk].map(updates)
    for col in type3:
        updates = inc.set_index(nk)[col]
        mask = old.es_actual.astype(bool) & old[nk].isin(updates.index)
        delta = mask & old[col].astype(str).ne(old[nk].map(updates).astype(str))
        old.loc[delta, col + '_anterior'] = old.loc[delta, col]
        old.loc[delta, col] = old.loc[delta, nk].map(updates)
    changed_keys = merged.loc[changed, sk].astype(int)
    closing = old[sk].isin(changed_keys)
    if closing.any() and old.loc[closing, 'valido_desde'].ge(now).any():
        raise ValueError('Cambio SCD sin avance temporal')
    old.loc[closing, 'valido_hasta'] = now
    old.loc[closing, 'es_actual'] = False
    added = merged.loc[new | changed, [nk] + attrs].copy()
    added[sk] = np.arange(int(old[sk].max()) + 1, int(old[sk].max()) + 1 + len(added))
    added['valido_desde'] = now
    added['valido_hasta'] = END
    added['es_actual'] = True
    added['historia_observada'] = True
    for col in type3:
        added[col + '_anterior'] = merged.loc[new | changed, col + '_old'].fillna('SIN_HISTORIA').to_numpy()
    return pd.concat([old, added], ignore_index=True)[old.columns]


def add_unknown(frame, nk, sk):
    """Miembro 0 DESCONOCIDO: los hechos sin resolución apuntan a él en vez de perderse."""
    if frame[sk].eq(0).any():
        return frame
    row = {c: ('DESCONOCIDO' if pd.api.types.is_string_dtype(frame[c]) else 0) for c in frame}
    row.update({nk: 'DESCONOCIDO', sk: 0, 'valido_desde': BASELINE, 'valido_hasta': END,
                'es_actual': True, 'historia_observada': False})
    if 'acepta_marketing' in frame:
        row['acepta_marketing'] = False
    return pd.concat([pd.DataFrame([row]), frame], ignore_index=True)


def temporal_lookup(frame, nk_values, dates, dim, nk, sk):
    """Asigna la versión vigente en fecha del hecho, sin usar siempre el cliente actual."""
    left = pd.DataFrame({'_order': np.arange(len(frame)),
                         nk: nk_values.fillna('DESCONOCIDO').astype(str).to_numpy(),
                         '_event': pd.to_datetime(dates).to_numpy()})
    if left._event.isna().any():
        raise ValueError('Fecha inválida antes del lookup temporal')
    right = dim[[nk, sk, 'valido_desde', 'valido_hasta']].copy()
    right[nk] = right[nk].astype(str)
    left['_event'] = left['_event'].astype('datetime64[ns]')
    right['valido_desde'] = pd.to_datetime(right.valido_desde).astype('datetime64[ns]')
    right['valido_hasta'] = pd.to_datetime(right.valido_hasta).astype('datetime64[ns]')
    matched = pd.merge_asof(left.sort_values('_event'), right.sort_values('valido_desde'),
                            left_on='_event', right_on='valido_desde', by=nk, direction='backward')
    valid = matched._event.lt(matched.valido_hasta)
    matched[sk] = matched[sk].where(valid, 0).fillna(0).astype('int64')
    return matched.sort_values('_order')[sk].to_numpy()


def validate_scd(dim, nk, sk):
    """Clave sustituta única, una versión vigente por NK e intervalos sin solapamiento."""
    assert dim[sk].is_unique
    assert dim.loc[dim.es_actual.astype(bool), nk].is_unique
    assert dim.valido_desde.lt(dim.valido_hasta).all()
    ordered = dim.sort_values([nk, 'valido_desde'])
    next_start = ordered.groupby(nk).valido_desde.shift(-1)
    assert (next_start.isna() | ordered.valido_hasta.le(next_start)).all()
