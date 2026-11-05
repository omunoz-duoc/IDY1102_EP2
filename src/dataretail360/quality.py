"""Auditoría de calidad: cada regla produce evidencia y ninguna fila se pierde sin contabilizar."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .observability import pipeline_step, quality_event


class QualityAuditor:
    """Acumula el balance por etapa (`quality`) y el linaje de rechazos (`reject_parts`)."""

    def __init__(self, log: logging.Logger, rejects_dir: Path | None):
        self.log = log
        self.rejects_dir = rejects_dir
        self.quality: list[dict] = []
        self.reject_parts: list[pd.DataFrame] = []

    def audit(self, df: pd.DataFrame, table: str, rules: dict, keys=None) -> pd.DataFrame:
        """Una fila rechazada puede tener varios motivos; se cuenta una sola vez."""
        with pipeline_step(self.log, 'silver.auditar_conjunto', object=str(table)) as step:
            result = self._audit(df, table, rules, keys)
            step.progress(rows=len(result))
            return result

    def _audit(self, df, table, rules, keys):
        df = df.copy()
        reasons = pd.Series('', index=df.index, dtype='str')
        for reason, mask in rules.items():
            mask = pd.Series(mask, index=df.index).fillna(True)
            reasons.loc[mask] = reasons.loc[mask] + reason + ';'
        bad = reasons.ne('')
        rejected = df.loc[bad].copy()
        rejected['_motivo'] = reasons[bad]
        remaining = df.loc[~bad].copy()
        duplicates = 0
        if keys:
            payload = [c for c in remaining if c not in ['_src_row', '_batch', '_fecha_carga']]
            exact = remaining.duplicated(payload)
            dup = remaining.loc[exact].copy()
            dup['_motivo'] = 'duplicado_exacto'
            duplicates = len(dup)
            remaining = remaining.loc[~exact].copy()
            conflict = remaining.duplicated(keys, keep=False)
            conflicting = remaining.loc[conflict].copy()
            conflicting['_motivo'] = 'clave_conflictiva'
            remaining = remaining.loc[~conflict].copy()
            rejected = pd.concat([rejected, conflicting], ignore_index=True)
            if len(dup) and self.rejects_dir is not None:
                dup.to_parquet(self.rejects_dir / f'{table}_duplicados.parquet', index=False)
        if len(rejected):
            if self.rejects_dir is not None:
                rejected.to_parquet(self.rejects_dir / f'{table}.parquet', index=False)
            self.reject_parts.append(rejected.assign(_tabla=table)[['_tabla', '_src_row', '_batch', '_motivo']])
        self.quality.append({'etapa': table, 'leidas': len(df), 'aceptadas': len(remaining),
                             'rechazadas': len(rejected), 'duplicadas': duplicates})
        assert len(df) == len(remaining) + len(rejected) + duplicates
        quality_event(self.log, table, len(df), len(remaining), len(rejected), duplicates)
        return remaining.reset_index(drop=True)

    def quality_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.quality)

    def rejections_frame(self) -> pd.DataFrame:
        if self.reject_parts:
            return pd.concat(self.reject_parts, ignore_index=True)
        return pd.DataFrame(columns=['_tabla', '_src_row', '_batch', '_motivo'])
