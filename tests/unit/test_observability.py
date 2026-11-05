from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import io,json,logging,os,subprocess,sys,tempfile,time,unittest
from dataretail360.observability import (configure_logging,pipeline_step,event,quality_event,
    trace_step,run_logged_command,CommandFailed,insert_progress,report_data_warnings)


class LoggingTest(unittest.TestCase):
    def setUp(self):
        base=Path(tempfile.gettempdir())/'dr360-logging-tests';base.mkdir(parents=True,exist_ok=True)
        self.temp=TemporaryDirectory(dir=base);self.root=Path(self.temp.name);self.output=io.StringIO()
        self.log=configure_logging(self.root,'TEST',stream=self.output,secrets=['private-test-password'],
            heartbeat_seconds=.015,slow_seconds=.035)

    def tearDown(self):
        for handler in self.log.handlers[:]:self.log.removeHandler(handler);handler.close()
        self.temp.cleanup()

    def records(self):
        return [json.loads(line) for line in (self.root/'logs/TEST.jsonl').read_text(encoding='utf-8').splitlines()]

    def test_success_progress_and_publication(self):
        @trace_step(self.log,'publicar',publication_arg=0)
        def publish(rid):
            with pipeline_step(self.log,'tabla',object='fact_ventas',rows_total=100) as s:s.progress(rows=50)
        publish('TEST_repeat')
        events=self.records();row=next(r for r in events if r['event']=='step_progress')
        self.assertEqual(row['publication_id'],'TEST_repeat');self.assertEqual(row['progress_pct'],50)
        self.assertTrue(all(r['run_id']=='TEST' for r in events))

    def test_decorator_preserves_keyword_call(self):
        @trace_step(self.log,'publicar',publication_arg=0)
        def publish(run_id):return run_id
        self.assertEqual(publish(run_id='KEYWORD'),'KEYWORD')
        self.assertEqual(self.records()[-1]['publication_id'],'KEYWORD')

    def test_failure_reraises_and_omits_sensitive_exception_text(self):
        with self.assertRaises(ValueError):
            with pipeline_step(self.log,'sql'):
                raise ValueError('RUT=PERSONAL; private-test-password; SQL parameters confidential')
        text=(self.root/'logs/TEST.jsonl').read_text(encoding='utf-8')
        self.assertNotIn('PERSONAL',text);self.assertNotIn('confidential',text)
        last=self.records()[-1]
        self.assertEqual(last['event'],'step_failed');self.assertEqual(last['error_type'],'ValueError')
        self.assertTrue(last['error_frames'])

    def test_redacts_stream_and_both_files(self):
        event(self.log,'probe','private-test-password password=abcde postgres://u:abcd@localhost')
        for text in [self.output.getvalue(),*(p.read_text(encoding='utf-8') for p in (self.root/'logs').iterdir())]:
            self.assertNotIn('private-test-password',text);self.assertNotIn('abcde',text);self.assertNotIn(':abcd@',text)

    def test_reconfiguration_does_not_duplicate_handlers(self):
        configure_logging(self.root,'TEST',stream=self.output,heartbeat_seconds=0)
        event(self.log,'unique','solo una vez')
        self.assertEqual(self.output.getvalue().count('solo una vez'),1)
        self.assertEqual(len(self.log.handlers),3)

    def test_heartbeat_and_slow_warning_stop(self):
        with pipeline_step(self.log,'largo'):time.sleep(.09)
        kinds=[r['event'] for r in self.records()]
        self.assertIn('heartbeat',kinds);self.assertEqual(kinds.count('slow_step'),1)
        before=len(self.records());time.sleep(.04);self.assertEqual(len(self.records()),before)

    def test_interruption_has_no_success(self):
        with self.assertRaises(KeyboardInterrupt):
            with pipeline_step(self.log,'cancelar'):raise KeyboardInterrupt()
        self.assertEqual(self.records()[-1]['event'],'step_cancelled')
        self.assertNotIn('step_done',[r['event'] for r in self.records()])

    def test_quality_warning_contains_counts_not_rows(self):
        quality_event(self.log,'origen',10,7,2,1)
        row=self.records()[-1]
        self.assertEqual(row['level'],'WARNING');self.assertEqual(row['rejected'],2)
        self.assertEqual(row['duplicates'],1)

    def test_only_leaf_emits_heartbeat(self):
        with pipeline_step(self.log,'padre'):
            with pipeline_step(self.log,'hijo'):time.sleep(.06)
        beats=[r['stage'] for r in self.records() if r['event']=='heartbeat']
        self.assertTrue(beats);self.assertEqual(set(beats),{'hijo'})

    def command(self,code,**kwargs):
        return run_logged_command(self.log,[sys.executable,'-u','-c',code],cwd=self.root,
            env=os.environ.copy(),log_path=self.root/'command.log',label='dbt.prueba',**kwargs)

    def test_subprocess_streams_progress_and_redacts(self):
        result=self.command("print('1 of 1 PASS private-test-password',flush=True)")
        self.assertEqual(result.returncode,0)
        self.assertIn('command_progress',[r['event'] for r in self.records()])
        self.assertNotIn('private-test-password',(self.root/'command.log').read_text())

    def test_subprocess_nonzero_is_not_success(self):
        with self.assertRaises(CommandFailed):self.command('raise SystemExit(2)')
        self.assertEqual(self.records()[-1]['event'],'step_failed')
        self.assertIn('command_failed',[r['event'] for r in self.records()])

    def test_expected_rejection_is_warning(self):
        with self.assertRaises(CommandFailed):self.command('raise SystemExit(1)',expected_failure=True)
        records=self.records()
        self.assertEqual(records[-1]['event'],'expected_rejection')
        self.assertNotIn('ERROR',[r['level'] for r in records])

    def test_subprocess_timeout_kills_owned_process(self):
        with self.assertRaises(subprocess.TimeoutExpired):self.command('import time;time.sleep(20)',timeout=.15)
        self.assertIn('command_timeout',[r['event'] for r in self.records()])
        self.assertEqual(self.records()[-1]['event'],'step_failed')

    def test_subprocess_silence_has_heartbeat(self):
        self.command('import time;time.sleep(.15)',timeout=5)
        self.assertIn('heartbeat',[r['event'] for r in self.records()])

    def test_observer_keeps_real_insert_and_unsubscribes_afterwards(self):
        import pandas as pd
        import sqlalchemy as sa
        engine=sa.create_engine('sqlite://')
        with engine.begin() as conn:
            with insert_progress(self.log,conn,'sample',5,every_rows=2):
                pd.DataFrame({'n':range(5)}).to_sql('sample',conn,index=False,chunksize=2)
            self.assertEqual(conn.scalar(sa.text('SELECT COUNT(*) FROM sample')),5)
            before=len(self.records())
            conn.execute(sa.text('INSERT INTO sample VALUES(99)'))
            self.assertEqual(len(self.records()),before)
        engine.dispose()
        seen=[r['rows'] for r in self.records() if r['event']=='step_progress']
        self.assertEqual(seen,[2,4,5])

    def test_observer_is_removed_when_insert_fails(self):
        import sqlalchemy as sa
        engine=sa.create_engine('sqlite://')
        with engine.begin() as conn:
            before=len(conn.dispatch.after_cursor_execute)
            with self.assertRaises(ValueError):
                with insert_progress(self.log,conn,'sample',1):
                    self.assertEqual(len(conn.dispatch.after_cursor_execute),before+1)
                    raise ValueError('controlled')
            self.assertEqual(len(conn.dispatch.after_cursor_execute),before)
            # Otra operación sigue funcionando después del error y no recibe callbacks viejos.
            self.assertEqual(conn.scalar(sa.text('SELECT 1')),1)
        engine.dispose()

    def test_subprocess_interruption_stops_command(self):
        with patch('dataretail360.observability.queue.Queue.get',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):self.command('import time;time.sleep(20)',timeout=5)
        self.assertEqual(self.records()[-1]['event'],'step_cancelled')

    def test_data_flags_are_aggregate_and_anonymous_is_information(self):
        import pandas as pd
        report_data_warnings(self.log,{
            'fact_ventas':pd.DataFrame({'cliente_key':[0,9],'producto_key':[1,2]}),
            'fact_marketing':pd.DataFrame({'sin_consentimiento':[True,False]})},ambiguous_count=3)
        rows=self.records()
        anonymous=next(r for r in rows if r.get('metric')=='ventas_sin_identidad')
        self.assertEqual(anonymous['level'],'INFO');self.assertEqual(anonymous['count'],1)
        self.assertEqual(next(r for r in rows if r.get('metric')=='correos_ambiguos')['count'],3)


if __name__=='__main__':unittest.main()
