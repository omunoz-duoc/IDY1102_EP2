"""Verifica el control de publicación exacto del notebook, sin conectarse a bases externas."""
from contextlib import contextmanager
from datetime import datetime,timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import ast,io,json,logging,unittest
import os,subprocess
from unittest.mock import Mock,patch
import pandas as pd
from pipeline_logging import configure_logging,trace_step,pipeline_step,event


class NotebookLoggingTest(unittest.TestCase):
    def setUp(self):
        base=Path.cwd()/'tmp'/'logging-tests';base.mkdir(parents=True,exist_ok=True)
        self.temp=TemporaryDirectory(dir=base);self.root=Path(self.temp.name)
        self.log=configure_logging(self.root,'ROOT',stream=io.StringIO(),heartbeat_seconds=0)
        nb=json.loads(Path('EP3_DataRetail360.ipynb').read_text(encoding='utf-8'))
        self.cells={c['id']:''.join(c['source']) for c in nb['cells'] if c['cell_type']=='code'}
        code=''.join(next(c for c in nb['cells'] if c['id']=='eb0419ed')['source'])
        node=next(n for n in ast.walk(ast.parse(code)) if isinstance(n,ast.FunctionDef) and n.name=='publish')
        self.actions=[];self.fail=None;self.audit_fail=False;self.cleanup_fail=False;self.statuses=[]
        case=self
        class Result:
            def scalar(self):return 0
        class Conn:
            def execute(self,sql,params=None):
                case.actions.append(str(sql).strip().splitlines()[0])
                if case.fail=='insert' and str(sql).startswith('INSERT INTO dw.fact_ventas'):
                    raise ValueError('Sensitive SQL row must not reach logs')
                if case.fail=='cancel' and str(sql).startswith('INSERT INTO dw.fact_ventas'):raise KeyboardInterrupt()
                if str(sql).startswith('UPDATE ctl.ejecuciones SET estado=:status'):
                    case.statuses.append(params['status'])
                    if case.audit_fail:raise RuntimeError('Audit unavailable')
                return Result()
            def scalar(self,sql,params=None):
                if 'sys.identity_columns' in sql:return 0
                if 'COUNT_BIG' in sql:return 2
                if 'SUM(monto_neto)' in sql:return 30
                raise AssertionError(sql)
        class Engine:
            @contextmanager
            def begin(self):
                case.actions.append('BEGIN')
                try:yield Conn()
                except BaseException:
                    case.actions.append('ROLLBACK');raise
                else:case.actions.append('COMMIT')
        self.runs={}
        def prepare(run_id):
            context={'source_schema':'stage_test','candidate_schema':'dbt_test','artifact_path':'artifacts/test'}
            self.runs[run_id]=context
            if self.fail=='prepare':raise ValueError('Candidate failed')
            return context
        def cleanup(run_id):
            self.actions.append('CLEANUP')
            if self.cleanup_fail:raise RuntimeError('cleanup failed')
        frames={'fact_ventas':pd.DataFrame({'monto_neto':[10,20]})}
        env={'LOG':self.log,'trace_step':trace_step,'pipeline_step':pipeline_step,'event':event,
            'logging':logging,'datetime':datetime,'timezone':timezone,'text':lambda sql:sql,
            'DW':Engine(),'facts':frames,'all_frames':frames,'physical_dimensions':{},'ROOT':self.root,
            'DBT_RUNS':self.runs,'prepare_dbt':prepare,'cleanup_dbt':cleanup,'json':json,
            'validate_destination':lambda frames:None,'clear_datamarts':lambda conn:None,
            'migrate_snowflake':lambda conn:None,'ensure_datamarts':lambda conn:None,
            'refresh_datamarts':lambda conn,rid:None}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'notebook_publish','exec'),env)
        self.env=env
        self.publish=env['publish']

    def tearDown(self):
        for handler in self.log.handlers[:]:self.log.removeHandler(handler);handler.close()
        self.temp.cleanup()

    def records(self):return [json.loads(s) for s in (self.root/'logs/ROOT.jsonl').read_text().splitlines()]

    def test_commit_event_follows_successful_transaction(self):
        self.publish('PUB')
        records=self.records();events=[r['event'] for r in records]
        tx_done=next(i for i,r in enumerate(records) if r['event']=='step_done' and r['stage']=='publicacion.transaccion_sql')
        self.assertLess(tx_done,events.index('publication_committed'))
        self.assertEqual(self.runs['PUB']['publication_status'],'OK')
        self.assertIn('CLEANUP',self.actions)
        self.assertTrue((self.root/'artifacts/test/publication.json').exists())

    def test_insert_error_rolls_back_and_never_logs_commit(self):
        self.fail='insert'
        with self.assertRaises(ValueError):self.publish('PUB')
        self.assertIn('ROLLBACK',self.actions);self.assertEqual(self.statuses,['FAILED'])
        self.assertNotIn('publication_committed',[r['event'] for r in self.records()])
        self.assertNotIn('Sensitive SQL row',json.dumps(self.records()))
        self.assertIn('CLEANUP',self.actions)

    def test_cancel_marks_cancelled_and_cleans(self):
        self.fail='cancel'
        with self.assertRaises(KeyboardInterrupt):self.publish('PUB')
        self.assertEqual(self.statuses,['CANCELLED'])
        self.assertIn('ROLLBACK',self.actions);self.assertIn('CLEANUP',self.actions)

    def test_prepare_error_does_not_start_publication_transaction(self):
        self.fail='prepare'
        with self.assertRaises(ValueError):self.publish('PUB')
        self.assertNotIn('DELETE FROM dw.fact_ventas',self.actions)
        self.assertEqual(self.statuses,['FAILED'])

    def test_cleanup_warning_keeps_confirmed_commit(self):
        self.cleanup_fail=True;self.publish('PUB')
        self.assertEqual(self.runs['PUB']['publication_status'],'OK')
        warning=next(r for r in self.records() if r['event']=='cleanup_pending')
        self.assertEqual(warning['level'],'WARNING')

    def test_secondary_audit_failure_preserves_original_exception(self):
        self.fail='insert';self.audit_fail=True
        with self.assertRaisesRegex(ValueError,'Sensitive SQL row'):self.publish('PUB')
        self.assertIn('audit_update_failed',[r['event'] for r in self.records()])

    def dbt_function(self,name,**extra):
        node=next(n for n in ast.walk(ast.parse(self.cells['dbt-orquestacion'])) if isinstance(n,ast.FunctionDef) and n.name==name)
        env={**self.env,"os":os,**extra}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'notebook_dbt','exec'),env)
        return env[name]

    def make_gate_artifacts(self):
        folder=self.root/'dbt_artifacts';folder.mkdir()
        nodes={f'model.m{i}':{'resource_type':'model','schema':'candidate','database':os.getenv('MSSQL_DATABASE','DataRetail360_DW')} for i in range(5)}
        nodes.update({f'test.t{i}':{'resource_type':'test'} for i in range(58)})
        graph={'nodes':nodes,'sources':{f'source.s{i}':{'schema':'stage'} for i in range(18)}}
        results={'results':[{'unique_id':key,'status':'success' if key.startswith('model.') else 'pass'} for key in nodes]}
        (folder/'manifest.json').write_text(json.dumps(graph),encoding='utf-8')
        (folder/'run_results.json').write_text(json.dumps(results),encoding='utf-8')
        return folder,results

    def test_real_notebook_dbt_gate_logs_all_tests_passed(self):
        folder,_=self.make_gate_artifacts()
        self.dbt_function('dbt_gate')(folder,'stage','candidate')
        result=next(r for r in self.records() if r['event']=='dbt_gate_passed')
        self.assertEqual(result['tests_passed'],58)

    def test_real_notebook_dbt_gate_accepts_configured_database(self):
        with patch.dict(os.environ,{'MSSQL_DATABASE':'DataRetail360_Demo'}):
            folder,_=self.make_gate_artifacts()
            self.dbt_function('dbt_gate')(folder,'stage','candidate')
            with patch.dict(os.environ,{'MSSQL_DATABASE':'DifferentDestination'}):
                with self.assertRaisesRegex(ValueError,'fuera del destino'):
                    self.dbt_function('dbt_gate')(folder,'stage','candidate')

    def test_real_notebook_dbt_gate_labels_expected_probe(self):
        folder,results=self.make_gate_artifacts();results['results']=results['results'][-1:]
        results['results'][0]['status']='fail'
        (folder/'run_results.json').write_text(json.dumps(results),encoding='utf-8')
        with self.assertRaises(ValueError):self.dbt_function('dbt_gate')(folder,'stage','candidate',expected_failure=True)
        row=next(r for r in self.records() if r['event']=='dbt_gate_rejected')
        self.assertEqual(row['level'],'WARNING');self.assertTrue(row['expected'])

    def test_notebook_command_uses_streaming_runner_without_source_password(self):
        runner=Mock(return_value=subprocess.CompletedProcess([],0,stdout='',stderr=''))
        function=self.dbt_function('dbt_command',run_logged_command=runner,os=os,
            DBT_PROJECT=self.root/'dbt',DBT_EXE=self.root/'dbt.exe',CUSTOMER_CUTOFF_KEY=20260930)
        with patch.dict(os.environ,{'PGPASSWORD':'private-source-password'}):
            function(['docs','generate','--static'],self.root/'command','stage','candidate')
        args,kwargs=runner.call_args
        self.assertNotIn('PGPASSWORD',kwargs['env'])
        self.assertEqual(kwargs['env']['DBT_CANDIDATE_SCHEMA'],'candidate')
        self.assertEqual(kwargs['label'],'dbt.docs.generate')
        self.assertEqual(kwargs['timeout'],2400)


if __name__=='__main__':unittest.main()
