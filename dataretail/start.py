"""Inicializa exclusivamente las bases demo y abre el notebook en JupyterLab."""
import os
import sys
import sqlalchemy as sa
import pandas as pd
from .config import ROOT, postgres_engine, sqlserver_engine
from .demo import VERSION, frames, write_files


def initialize():
    if (os.getenv('DATARETAIL_DEMO') != '1' or os.getenv('PGDATABASE') != 'dataretail_demo'
            or os.getenv('MSSQL_DATABASE') != 'DataRetail360_Demo'):
        raise RuntimeError('Inicialización permitida solo en las bases dedicadas de Docker demo.')
    dataset = frames()
    pg = postgres_engine()
    with pg.begin() as conn:
        conn.execute(sa.text('CREATE SCHEMA IF NOT EXISTS dr360_control'))
        conn.execute(sa.text('CREATE TABLE IF NOT EXISTS dr360_control.demo_version (version text PRIMARY KEY)'))
        version = conn.scalar(sa.text('SELECT version FROM dr360_control.demo_version'))
        if version and version != VERSION:
            raise RuntimeError('Versión demo incompatible; consultar actualización de volúmenes en README.')
        if not version:
            if conn.scalar(sa.text("SELECT count(*) FROM information_schema.tables WHERE table_schema='dr360_raw'")):
                raise RuntimeError('Origen existente sin marcador demo; no se sobrescribe.')
            conn.execute(sa.text('CREATE SCHEMA IF NOT EXISTS dr360_raw'))
            for name, frame in dataset.items():
                if name not in ('mkt_campanias', 'legacy_reporte_ventas_xlsx'):
                    frame.to_sql(name, conn, schema='dr360_raw', index=False, if_exists='fail')
            controls = {
                'config_generacion': pd.DataFrame([{'parametro':'fecha_fin','valor':'2026-09-30'},
                                                   {'parametro':'dataset','valor':VERSION}]),
                'definiciones_kpi_por_area': pd.DataFrame([
                    {'area':area,'indicador':metric,'definicion':definition} for area,metric,definition in [
                        ('Comercial','Venta neta','Ventas netas menos devoluciones netas'),
                        ('Operaciones','Stock','Último corte común'),
                        ('Marketing','Conversión','Conversiones entregadas / envíos entregados'),
                        ('Clientes','Actividad','Compras en seis meses calendario')]]),
                'problemas_calidad_inyectados': pd.DataFrame([
                    {'tabla':'legacy_reporte_ventas_xlsx','problema':'Fecha ambigua','filas':1},
                    {'tabla':'mkt_envios_campania','problema':'Sin consentimiento','filas':4}])}
            for name, frame in controls.items():
                frame.to_sql(name, conn, schema='dr360_control', index=False, if_exists='fail')
            conn.execute(sa.text('INSERT INTO dr360_control.demo_version VALUES (:v)'), {'v':VERSION})
    pg.dispose()
    marker=ROOT/'data'/'.demo-version'
    if marker.exists() and marker.read_text().strip() != VERSION:
        raise RuntimeError('Archivos demo de otra versión; consultar README.')
    if not marker.exists():
        if any((ROOT/'data'/'raw').rglob('*.*')):
            raise RuntimeError('Archivos raw existentes sin marcador demo; no se sobrescriben.')
        write_files(ROOT, dataset)
        marker.write_text(VERSION, encoding='utf-8')
    master=sqlserver_engine('master')
    with master.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
        conn.execute(sa.text("IF DB_ID(N'DataRetail360_Demo') IS NULL CREATE DATABASE [DataRetail360_Demo]"))
    master.dispose()
    print('Dataset sintético disponible; las reejecuciones conservan la historia del DWH.', flush=True)


def main():
    initialize()
    os.chdir(ROOT)
    os.execv(sys.executable, [sys.executable, '-m', 'jupyterlab', '--ip=0.0.0.0', '--port=8888',
        '--no-browser', '--ServerApp.default_url=/lab/tree/EP3_DataRetail360.ipynb'])


if __name__ == '__main__':
    main()
