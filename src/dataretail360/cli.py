"""Línea de comandos: `dataretail360 <comando>` o `python -m dataretail360 <comando>`."""
from __future__ import annotations

import argparse
import sys

from .settings import Paths, dbt_executable, dw_database, load_env


def cmd_doctor(args) -> int:
    """Verifica rutas, dbt y conectividad a PostgreSQL y SQL Server sin modificar datos."""
    from sqlalchemy import text
    from .settings import postgres_engine, sqlserver_engine
    paths = Paths.from_env()
    print(f'home        : {paths.home}')
    print(f'dbt project : {paths.dbt_project} ({"ok" if (paths.dbt_project / "dbt_project.yml").is_file() else "AUSENTE"})')
    ok = True
    try:
        print(f'dbt         : {dbt_executable(paths)}')
    except FileNotFoundError as exc:
        ok = False
        print(f'dbt         : ERROR {exc}')
    for label, factory, sql in [('postgresql', postgres_engine, 'SELECT current_database()'),
                                ('sqlserver', sqlserver_engine, 'SELECT DB_NAME()')]:
        try:
            engine = factory()
            with engine.connect() as conn:
                print(f'{label:<12}: ok ({conn.scalar(text(sql))})')
            engine.dispose()
        except Exception as exc:  # diagnóstico: informar tipo sin texto sensible
            ok = False
            print(f'{label:<12}: ERROR {type(exc).__name__}')
    print(f'DW esperado : {dw_database()}')
    return 0 if ok else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='dataretail360', description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    doctor = sub.add_parser('doctor', help='Verificar configuración y conexiones.')
    doctor.set_defaults(func=cmd_doctor)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
