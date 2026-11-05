"""Reporte de ejecución en Markdown.

Cada etapa agrega texto y tablas; al final se escribe `artifacts/reports/<run_id>/report.md`.
No requiere dependencias adicionales (el render de tablas es propio).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def _cell(value) -> str:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return ''
    if isinstance(value, float):
        text = f'{value:,.6f}'.rstrip('0').rstrip('.')
    else:
        text = str(value)
    return text.replace('|', '\\|').replace('\n', ' ')


def markdown_table(frame: pd.DataFrame, max_rows: int | None = 50) -> str:
    shown = frame if max_rows is None else frame.head(max_rows)
    columns = [str(c) for c in shown.columns]
    lines = ['| ' + ' | '.join(columns) + ' |', '|' + '---|' * len(columns)]
    for row in shown.itertuples(index=False, name=None):
        lines.append('| ' + ' | '.join(_cell(v) for v in row) + ' |')
    if max_rows is not None and len(frame) > max_rows:
        lines.append(f'\n_… {len(frame) - max_rows} filas adicionales omitidas._')
    return '\n'.join(lines)


class Report:
    def __init__(self, title: str):
        self.blocks: list[str] = [f'# {title}']

    def section(self, title: str, level: int = 2) -> None:
        self.blocks.append('#' * level + ' ' + title)

    def text(self, markdown: str) -> None:
        self.blocks.append(markdown.strip())

    def table(self, frame: pd.DataFrame, title: str | None = None, max_rows: int | None = 50) -> None:
        if title:
            self.blocks.append(f'**{title}**')
        self.blocks.append(markdown_table(frame, max_rows))

    def details(self, summary: str, body: str) -> None:
        self.blocks.append(f'<details><summary>{summary}</summary>\n\n{body.strip()}\n\n</details>')

    def image(self, path: Path, relative_to: Path, caption: str) -> None:
        self.blocks.append(f'![{caption}]({Path(path).resolve().relative_to(relative_to.resolve()).as_posix()})')

    def render(self) -> str:
        return '\n\n'.join(self.blocks) + '\n'

    def save(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.render(), encoding='utf-8')
        return path
