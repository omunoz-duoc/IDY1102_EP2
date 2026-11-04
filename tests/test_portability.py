"""Contratos demo y ejecución real de Silver/dimensiones/hechos sin servicios externos."""
import ast
import hashlib
import io
import json
import logging
import re
import unicodedata
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import numpy as np
import pandas as pd
import sqlalchemy as sa
from dataretail.config import ROOT, sqlserver_engine
from dataretail.demo import frames, write_files, SHEET
from dataretail.contracts import CONTRACT
from pipeline_logging import (configure_logging, pipeline_step, trace_step,
                              quality_event, report_data_warnings)


class PortabilityTest(unittest.TestCase):
    def test_synthetic_sources_are_repeatable_and_complete(self):
        first, second = frames(), frames()
        self.assertEqual(set(first), set(CONTRACT))
        for name, frame in first.items():
            self.assertFalse(frame.empty, name)
            self.assertEqual(list(frame.columns), CONTRACT[name])
            pd.testing.assert_frame_equal(frame, second[name])

    def test_sql_authentication_and_custom_database(self):
        with patch.dict('os.environ', {'MSSQL_AUTH':'sql', 'MSSQL_USER':'demo',
                        'MSSQL_PASSWORD':'test-only', 'MSSQL_SERVER':'sqlserver',
                        'MSSQL_PORT':'1433', 'MSSQL_DATABASE':'DataRetail360_Demo'}):
            engine=sqlserver_engine()
            self.assertEqual(engine.url.database, 'DataRetail360_Demo')
            self.assertEqual(engine.url.username, 'demo')
            self.assertEqual(engine.url.port, 1433)
            self.assertNotIn('Trusted_Connection', engine.url.query)
            engine.dispose()

    def test_demo_through_actual_notebook_transformations(self):
        nb=json.loads((ROOT/'EP3_DataRetail360.ipynb').read_text(encoding='utf-8'))
        (ROOT/'tmp').mkdir(exist_ok=True)
        with TemporaryDirectory(dir=ROOT/'tmp') as folder:
            root=Path(folder)
            data=root/'data'
            for name in ['silver','rechazos','control']:
                (data/name).mkdir(parents=True)
            raw=frames()
            write_files(root,raw)
            # Exercise the same CSV/XLSX textual readers used by Bronze.
            raw['mkt_campanias']=pd.read_csv(data/'raw/marketing/campanias.csv',dtype='string',keep_default_na=False)
            raw['legacy_reporte_ventas_xlsx']=pd.read_excel(data/'raw/legacy/reportes_ventas.xlsx',
                sheet_name=SHEET,dtype='string',keep_default_na=False)
            for frame in raw.values():
                frame['_src_row']=np.arange(1,len(frame)+1)
                frame['_batch']='test-demo'
            source=''.join(nb['cells'][7]['source'])
            keys=next(node for node in ast.parse(source).body if isinstance(node,ast.Assign)
                      and isinstance(node.targets[0],ast.Name) and node.targets[0].id=='KEYS')
            log=configure_logging(root,'demo-test',stream=io.StringIO(),heartbeat_seconds=0)
            engine=sa.create_engine('sqlite://')
            env=dict(pd=pd,np=np,sa=sa,text=sa.text,datetime=datetime,hashlib=hashlib,
                re=re,unicodedata=unicodedata,raw=raw,KEYS=ast.literal_eval(keys.value),
                LOG=log,DATA=data,DW=engine,pipeline_step=pipeline_step,trace_step=trace_step,
                quality_event=quality_event,report_data_warnings=report_data_warnings,
                display=lambda *args:None,Markdown=lambda value:value)
            try:
                for index in [11,12,13,16,17,18,19,20,22,23,24]:
                    exec(compile(''.join(nb['cells'][index]['source']),f'notebook-cell-{index}','exec'),env)
                facts=env['facts']
                self.assertEqual(len(facts['fact_ventas']),64)
                self.assertEqual(len(facts['fact_devoluciones']),5)
                self.assertEqual(len(facts['fact_inventario']),36)
                self.assertTrue(facts['fact_ventas'].cliente_key.eq(0).any())
                self.assertTrue(facts['fact_devoluciones'].fecha_key.gt(20260930).any())
                self.assertTrue(all(len(frame)>0 for frame in facts.values()))
                self.assertEqual(int(facts['fact_ventas'].monto_neto.sum()),192000)
            finally:
                engine.dispose()
                for handler in list(log.handlers):
                    handler.close();log.removeHandler(handler)


if __name__=='__main__':
    unittest.main()
