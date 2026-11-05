"""Configuración del pipeline: rutas de estado, conexiones y ejecutable dbt.

Sin efectos secundarios al importar. Las credenciales provienen del entorno (o de `.env`),
nunca del código (variables estándar PG* y MSSQL_*).
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

import sqlalchemy as sa

# Raíz del código fuente (útil al ejecutar desde un clon sin instalar el paquete).
SOURCE_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DW_DATABASE = 'DataRetail360_DW'


def load_env(home: Path | None = None) -> None:
    """Carga `.env` del directorio de trabajo sin sobrescribir variables ya definidas."""
    from dotenv import load_dotenv
    for candidate in [home, Path.cwd()]:
        if candidate and (candidate / '.env').is_file():
            load_dotenv(candidate / '.env', encoding='utf-8-sig', override=False)
            return


@dataclass(frozen=True)
class Paths:
    """Estado de ejecución. `home` contiene data/, artifacts/ y logs/; el código es inmutable."""
    home: Path
    dbt_project: Path

    @property
    def data(self) -> Path: return self.home / 'data'
    @property
    def raw(self) -> Path: return self.data / 'raw'
    @property
    def bronze(self) -> Path: return self.data / 'bronze'
    @property
    def silver(self) -> Path: return self.data / 'silver'
    @property
    def gold(self) -> Path: return self.data / 'gold'
    @property
    def control(self) -> Path: return self.data / 'control'
    @property
    def rejects(self) -> Path: return self.data / 'rechazos'
    @property
    def artifacts(self) -> Path: return self.home / 'artifacts'
    @property
    def logs(self) -> Path: return self.home / 'logs'

    def ensure(self) -> None:
        for folder in [self.bronze, self.silver, self.gold, self.control, self.rejects, self.logs]:
            folder.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> 'Paths':
        home = Path(os.getenv('DATARETAIL_HOME') or Path.cwd()).resolve()
        dbt = os.getenv('DBT_PROJECT_DIR')
        if dbt:
            project = Path(dbt).resolve()
        elif (home / 'dbt' / 'dbt_project.yml').is_file():
            project = home / 'dbt'
        else:
            project = SOURCE_ROOT / 'dbt'
        return cls(home=home, dbt_project=project)


def dw_database() -> str:
    return os.getenv('MSSQL_DATABASE', DEFAULT_DW_DATABASE)


def postgres_engine():
    if not os.getenv('PGPASSWORD'):
        raise RuntimeError('Falta PGPASSWORD. Usar Docker Compose o configurar .env; ver README.')
    return sa.create_engine(sa.URL.create(
        'postgresql+psycopg', username=os.getenv('PGUSER', 'postgres'),
        password=os.environ['PGPASSWORD'], host=os.getenv('PGHOST', 'localhost'),
        port=int(os.getenv('PGPORT', '5432')), database=os.getenv('PGDATABASE', 'idy1102_ep2')),
        hide_parameters=True, connect_args={'connect_timeout': 10}, pool_pre_ping=True)


def sqlserver_engine(database: str | None = None):
    auth = os.getenv('MSSQL_AUTH', 'sql').lower()
    if auth not in ('sql', 'windows'):
        raise ValueError('MSSQL_AUTH debe ser sql o windows')
    query = {'driver': os.getenv('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server'),
             'Encrypt': 'yes', 'TrustServerCertificate': os.getenv('MSSQL_TRUST_CERT', 'yes')}
    credentials = {}
    if auth == 'windows':
        query['Trusted_Connection'] = 'yes'
    else:
        credentials = {'username': os.environ['MSSQL_USER'], 'password': os.environ['MSSQL_PASSWORD']}
    port = os.getenv('MSSQL_PORT', '')
    return sa.create_engine(sa.URL.create(
        'mssql+pyodbc', host=os.getenv('MSSQL_SERVER', 'localhost'),
        port=int(port) if port else None,
        database=database or dw_database(),
        query=query, **credentials), fast_executemany=True, hide_parameters=True, pool_pre_ping=True)


def dbt_executable(paths: Paths | None = None) -> Path:
    paths = paths or Paths.from_env()
    candidates = [os.getenv('DBT_EXECUTABLE'),
                  str(paths.home / '.venv-dbt' / 'bin' / 'dbt'),
                  str(paths.home / '.venv-dbt' / 'Scripts' / 'dbt.exe'), shutil.which('dbt')]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return Path(candidate)
    raise FileNotFoundError('dbt no disponible. Definir DBT_EXECUTABLE o usar Docker (ver README.md).')


def secrets_from_env() -> list[str]:
    """Valores a ocultar en logs y salidas de comandos."""
    return [value for key, value in os.environ.items()
            if any(token in key.upper() for token in ('PASSWORD', 'TOKEN', 'SECRET'))]
