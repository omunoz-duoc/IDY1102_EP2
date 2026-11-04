"""Conexiones y rutas compartidas; sin efectos secundarios al importar."""
import os
import shutil
from pathlib import Path
import sqlalchemy as sa


ROOT = Path(__file__).resolve().parents[1]


def postgres_engine():
    return sa.create_engine(sa.URL.create(
        'postgresql+psycopg', username=os.getenv('PGUSER', 'postgres'),
        password=os.environ['PGPASSWORD'], host=os.getenv('PGHOST', 'localhost'),
        port=int(os.getenv('PGPORT', '5432')), database=os.getenv('PGDATABASE', 'idy1102_ep2')),
        hide_parameters=True, connect_args={'connect_timeout': 10}, pool_pre_ping=True)


def sqlserver_engine(database=None):
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
        database=database or os.getenv('MSSQL_DATABASE', 'DataRetail360_DW'),
        query=query, **credentials), fast_executemany=True, hide_parameters=True, pool_pre_ping=True)


def dbt_executable():
    candidates = [os.getenv('DBT_EXECUTABLE'),
                  str(ROOT / '.venv-dbt' / 'bin' / 'dbt'),
                  str(ROOT / '.venv-dbt' / 'Scripts' / 'dbt.exe'), shutil.which('dbt')]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return Path(candidate)
    raise FileNotFoundError('dbt no disponible. Iniciar el entorno Docker descrito en README.md.')
