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


def cmd_run(args) -> int:
    """Ejecuta el pipeline completo y escribe el reporte de la corrida."""
    from .context import RunContext
    from .pipeline import run
    ctx = RunContext.create(run_id=args.run_id)
    try:
        run(ctx, verify=not args.skip_verify)
    finally:
        path = ctx.report.save(ctx.report_path)
        ctx.log.info('Reporte de la corrida: %s', path)
        ctx.close()
    return 0


def cmd_diagram(args) -> int:
    from pathlib import Path
    from .analysis import bus_matrix_figure
    Path(args.output).write_bytes(bus_matrix_figure())
    print(f'Matriz de bus escrita en {args.output}')
    return 0


def cmd_seed(args) -> int:
    """Prepara las fuentes del pipeline (PostgreSQL + CSV + Excel) y la base destino."""
    from pathlib import Path
    from .seed import bootstrap
    if args.action == 'demo':
        bootstrap.seed_demo(replace=args.replace)
    elif args.action == 'export':
        bootstrap.export_bundle(Path(args.path))
    elif args.action == 'load':
        bootstrap.load_bundle(Path(args.path), replace=args.replace)
    elif args.action == 'verify':
        manifest = bootstrap.verify_bundle(Path(args.path))
        print(f'Paquete íntegro: {len(manifest["tables"])} tablas y {len(manifest["files"])} archivos.')
    elif args.action == 'none':
        created = bootstrap.ensure_target_database()
        print('Fuentes sin cambios; base destino ' + ('creada.' if created else 'existente.'))
    elif args.action == 'contract':
        from .report import markdown_table
        print(markdown_table(bootstrap.contract_report(), max_rows=None))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='dataretail360', description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    doctor = sub.add_parser('doctor', help='Verificar configuración y conexiones.')
    doctor.set_defaults(func=cmd_doctor)
    run = sub.add_parser('run', help='Ejecutar el pipeline completo (extracción → DWH → marts → Gold).')
    run.add_argument('--run-id', help='Identificador de corrida (por defecto, UUID aleatorio).')
    run.add_argument('--skip-verify', action='store_true',
                     help='Omitir la segunda publicación, sondas de rollback/dbt y benchmark.')
    run.set_defaults(func=cmd_run)
    seed = sub.add_parser('seed', help='Preparar las fuentes de entrada del pipeline.')
    seed_sub = seed.add_subparsers(dest='action', required=True)
    demo = seed_sub.add_parser('demo', help='Sembrar el dataset sintético determinista (idempotente).')
    demo.add_argument('--replace', action='store_true', help='Reemplazar fuentes existentes.')
    export = seed_sub.add_parser('export', help='Congelar las fuentes actuales en un paquete portable.')
    export.add_argument('path', help='Carpeta vacía de destino del paquete.')
    load = seed_sub.add_parser('load', help='Reconstruir las fuentes desde un paquete.')
    load.add_argument('path', help='Carpeta del paquete (con manifest.json).')
    load.add_argument('--replace', action='store_true', help='Reemplazar fuentes existentes.')
    verify = seed_sub.add_parser('verify', help='Verificar integridad (SHA256) y completitud de un paquete.')
    verify.add_argument('path')
    seed_sub.add_parser('none', help='No sembrar; solo asegurar que exista la base destino.')
    seed_sub.add_parser('contract', help='Mostrar el contrato de entrada requerido.')
    seed.set_defaults(func=cmd_seed)
    diagram = sub.add_parser('diagram', help='Generar la matriz de bus del modelo dimensional (PNG).')
    diagram.add_argument('output', nargs='?', default='modelo_bus_matrix.png')
    diagram.set_defaults(func=cmd_diagram)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
