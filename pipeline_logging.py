"""Observabilidad del notebook: progreso legible, eventos JSONL y errores sin datos de filas.

Solo biblioteca estándar. Los logs informan actividad, no consultan ni modifican datos.
Un heartbeat confirma que el proceso Python sigue vivo; no certifica avance del servidor SQL.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
import json
import inspect
import logging
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
import traceback

_CURRENT = ContextVar('dr360_log_step', default=None)
_FIELDS = ('event','stage','publication_id','object','source_type','rows','rows_total',
           'accepted','rejected','duplicates','elapsed_s','rows_per_s','progress_pct',
           'returncode','error_type','error_frames','action','artifact','attempt','expected',
           'checks_passed','tests_passed','tables','process_id')


class CommandFailed(RuntimeError):
    def __init__(self,label,result):
        super().__init__(f'Falló {label}; código de salida {result.returncode}. Revisar el log del comando.')
        self.result=result


class Redactor:
    def __init__(self, secrets=()):
        self.secrets=sorted({str(s) for s in secrets if s and len(str(s))>=4},key=len,reverse=True)

    def __call__(self, value):
        text=str(value)
        for secret in self.secrets: text=text.replace(secret,'[REDACTED]')
        text=re.sub(r'(?i)(password|pwd|token|secret)(\s*[=:]\s*)([^;\s]+)',r'\1\2[REDACTED]',text)
        return re.sub(r'(?i)([a-z][a-z0-9+.-]*://[^/\s:@]+:)[^@\s]+@',r'\1[REDACTED]@',text)


class _ContextFilter(logging.Filter):
    def __init__(self,run_id):
        super().__init__();self.run_id=run_id

    def filter(self,record):
        current=_CURRENT.get()
        record.run_id=self.run_id
        if not hasattr(record,'stage'):record.stage=current.name if current else 'pipeline'
        if current and not hasattr(record,'publication_id') and 'publication_id' in current.fields:
            record.publication_id=current.fields['publication_id']
        return True


class _Formatter(logging.Formatter):
    def __init__(self, redactor, structured=False):
        super().__init__();self.redactor=redactor;self.structured=structured

    def format(self,record):
        message=self.redactor(record.getMessage())
        fields={key:getattr(record,key) for key in _FIELDS if hasattr(record,key)}
        fields.setdefault('event','message')
        fields={key:self.redactor(value) if isinstance(value,str) else value for key,value in fields.items()}
        stamp=datetime.fromtimestamp(record.created,timezone.utc).isoformat(timespec='milliseconds')
        if self.structured:
            return json.dumps({'timestamp_utc':stamp,'level':record.levelname,'run_id':record.run_id,
                'message':message,**fields},ensure_ascii=False,default=str)
        details=' '.join(f'{key}={value}' for key,value in fields.items() if key not in ('stage','error_frames'))
        # No exc_info ni texto de excepciones SQL: podrían contener parámetros o filas.
        return f'{stamp} {record.levelname:<7} run={record.run_id} [{record.stage}] {message}'+(' | '+details if details else '')


def configure_logging(root,run_id,stream=None,secrets=(),heartbeat_seconds=30,slow_seconds=300):
    """Reejecutable: reemplaza handlers propios y evita líneas duplicadas en Jupyter."""
    folder=Path(root)/'logs';folder.mkdir(parents=True,exist_ok=True)
    logger=logging.getLogger('dr360');logger.setLevel(logging.DEBUG);logger.propagate=False
    for handler in logger.handlers[:]:logger.removeHandler(handler);handler.close()
    redact=Redactor(secrets)
    for target,structured in [(folder/f'{run_id}.log',False),(folder/f'{run_id}.jsonl',True)]:
        handler=logging.FileHandler(target,encoding='utf-8');handler.setLevel(logging.DEBUG)
        handler.setFormatter(_Formatter(redact,structured));handler.addFilter(_ContextFilter(run_id))
        logger.addHandler(handler)
    console=logging.StreamHandler(sys.stdout if stream is None else stream);console.setLevel(logging.INFO)
    console.setFormatter(_Formatter(redact));console.addFilter(_ContextFilter(run_id));logger.addHandler(console)
    logger.pipeline_heartbeat_seconds=float(heartbeat_seconds)
    logger.pipeline_slow_seconds=float(slow_seconds)
    logger.pipeline_redactor=redact
    event(logger,'run_started','Pipeline iniciado; progreso en pantalla y detalle en logs.',
        artifact=str(folder/f'{run_id}.jsonl'))
    return logger


def event(logger,code,message,level=logging.INFO,**fields):
    unknown=set(fields)-set(_FIELDS)
    if unknown:raise ValueError('Campo de log no permitido: '+','.join(sorted(unknown)))
    logger.log(level,message,extra={'event':code,**fields})


def error_event(logger,exc,**fields):
    frames=' > '.join(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
        for frame in traceback.extract_tb(exc.__traceback__))
    event(logger,'step_failed','La etapa falló; revisar contexto y artefactos antes de reintentar.',
        level=logging.ERROR,error_type=type(exc).__name__,error_frames=frames,**fields)


class Step:
    def __init__(self,logger,name,heartbeat=True,**fields):
        self.logger=logger;self.name=name;self.fields=fields
        self.interval=logger.pipeline_heartbeat_seconds if heartbeat else 0
        self.stop=threading.Event();self.lock=threading.Lock();self.children=0;self.rows=None
        self.warned=False;self.started=0;self.parent=None;self.thread=None

    def emit(self,code,message,level=logging.INFO,**fields):
        event(self.logger,code,message,level=level,stage=self.name,
            **{**self.fields,**fields})

    def progress(self,rows=None,**fields):
        with self.lock:
            if rows is not None:self.rows=int(rows)
        elapsed=time.monotonic()-self.started
        details={'elapsed_s':round(elapsed,3),**fields}
        if rows is not None:
            details['rows']=int(rows)
            if elapsed>0:details['rows_per_s']=round(rows/elapsed,1)
            total=self.fields.get('rows_total')
            if total and 0<=rows<=total:details['progress_pct']=round(100*rows/total,1)
        self.emit('step_progress','Avance confirmado.',**details)

    def _heartbeat(self):
        while not self.stop.wait(self.interval):
            with self.lock:
                if self.children:continue
                rows=self.rows
            elapsed=time.monotonic()-self.started
            self.emit('heartbeat','Sigue en ejecución; no hay un nuevo avance confirmado por la operación.',
                elapsed_s=round(elapsed,1),**({'rows':rows} if rows is not None else {}))
            if not self.warned and elapsed>=self.logger.pipeline_slow_seconds:
                self.warned=True
                self.emit('slow_step','La etapa supera el umbral de duración; esto no prueba que esté bloqueada.',
                    level=logging.WARNING,elapsed_s=round(elapsed,1),
                    action='Si persiste, revisar recursos, bloqueos SQL y log del comando. No iniciar otra publicación en paralelo.')

    def __enter__(self):
        self.started=time.monotonic();self.parent=_CURRENT.get()
        if self.parent:
            with self.parent.lock:self.parent.children+=1
        self.token=_CURRENT.set(self)
        if self.parent and 'publication_id' in self.parent.fields:
            self.fields.setdefault('publication_id',self.parent.fields['publication_id'])
        self.emit('step_started','Inicio de etapa.')
        if self.interval>0:
            self.thread=threading.Thread(target=self._heartbeat,name='dr360-log-heartbeat',daemon=True)
            self.thread.start()
        return self

    def __exit__(self,kind,exc,tb):
        self.stop.set()
        if self.thread:self.thread.join(timeout=1)
        elapsed=round(time.monotonic()-self.started,3)
        try:
            if exc is None:
                self.emit('step_done','Etapa terminada.',elapsed_s=elapsed,
                    **({'rows':self.rows} if self.rows is not None else {}))
            elif self.fields.get('expected') and isinstance(exc,CommandFailed):
                self.emit('expected_rejection','La prueba controlada obtuvo el rechazo esperado.',
                    level=logging.WARNING,elapsed_s=elapsed,error_type=type(exc).__name__)
            elif isinstance(exc,(KeyboardInterrupt,SystemExit)):
                self.emit('step_cancelled','Ejecución interrumpida; no se declara éxito.',
                    level=logging.WARNING,elapsed_s=elapsed,error_type=type(exc).__name__)
            else:
                error_event(self.logger,exc,stage=self.name,elapsed_s=elapsed,**self.fields)
        finally:
            _CURRENT.reset(self.token)
            if self.parent:
                with self.parent.lock:self.parent.children-=1
        return False


def pipeline_step(logger,name,**fields):return Step(logger,name,**fields)


def trace_step(logger,name,object_arg=None,publication_arg=None,expected=False):
    def decorate(function):
        signature=inspect.signature(function)
        parameters=list(signature.parameters)
        @wraps(function)
        def traced(*args,**kwargs):
            fields={'expected':True} if expected else {}
            bound=signature.bind_partial(*args,**kwargs)
            for index,field in [(object_arg,'object'),(publication_arg,'publication_id')]:
                if index is not None and parameters[index] in bound.arguments:
                    fields[field]=str(bound.arguments[parameters[index]])
            with pipeline_step(logger,name,**fields) as current:
                result=function(*args,**kwargs)
                candidate=result[0] if isinstance(result,tuple) and result else result
                if hasattr(candidate,'shape'):current.progress(rows=len(candidate))
                return result
        return traced
    return decorate


def quality_event(logger,table,read,accepted,rejected,duplicates):
    event(logger,'quality_summary','Balance de calidad por conjunto.',object=table,
        rows=int(read),accepted=int(accepted),rejected=int(rejected),duplicates=int(duplicates))
    if rejected or duplicates:
        event(logger,'quality_warning','Se separaron filas; revisar cuarentena y reglas antes de interpretar indicadores.',
            level=logging.WARNING,object=table,rejected=int(rejected),duplicates=int(duplicates),
            action='Consultar data/rechazos y la tabla de calidad; no es una falla de publicación.')


@contextmanager
def insert_progress(logger,connection,table,total,every_rows=50000):
    """Observa batches confirmados por el cursor; no cambia el insert ni agrega consultas SQL.

    Estos avances están dentro de la transacción: no equivalen a un commit del DWH.
    """
    import sqlalchemy as sa
    with pipeline_step(logger,'staging.insertar',object=table,rows_total=int(total)) as step:
        seen=0;reported=0
        def after_cursor(conn,cursor,statement,parameters,context,executemany):
            nonlocal seen,reported
            if not getattr(context,'isinsert',False):return
            seen+=len(parameters) if executemany else 1
            if seen-reported>=every_rows or seen==total:
                step.progress(rows=seen);reported=seen
        sa.event.listen(connection,'after_cursor_execute',after_cursor)
        try:
            yield step
            if seen!=reported:step.progress(rows=seen)
            if seen!=total:
                step.emit('batch_count_warning','El conteo observado por cursor difiere del esperado; la conciliación SQL decide el resultado.',
                    level=logging.WARNING,rows=seen)
        finally:
            sa.event.remove(connection,'after_cursor_execute',after_cursor)


def _terminate_tree(process):
    if process.poll() is not None:return True
    complete=True
    if os.name=='nt':
        # PID propio, argumentos separados; no se construye ni ejecuta una cadena de shell.
        result=subprocess.run(['taskkill','/PID',str(process.pid),'/T','/F'],capture_output=True,check=False)
        complete=result.returncode==0
        if process.poll() is None:process.kill()
    else:
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
    process.wait(timeout=10)
    return complete


def run_logged_command(logger,command,*,cwd,env,log_path,label,timeout=2400,expected_failure=False):
    """Drena stdout/stderr sin bloquear el heartbeat; timeout/interrupción detienen el árbol propio."""
    startup={'start_new_session':True} if os.name!='nt' else {
        'creationflags':subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW}
    log_path=Path(log_path);log_path.parent.mkdir(parents=True,exist_ok=True)
    lines=[];messages=queue.Queue();process=None;reader=None
    with pipeline_step(logger,label,artifact=str(log_path),expected=expected_failure) as step:
        try:
            process=subprocess.Popen(command,cwd=cwd,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                text=True,encoding='utf-8',errors='replace',bufsize=1,**startup)
            def drain():
                try:
                    for line in process.stdout:messages.put(line)
                finally:messages.put(None)
            reader=threading.Thread(target=drain,name='dr360-command-reader',daemon=True);reader.start()
            ended=False
            with log_path.open('w',encoding='utf-8') as output:
                while not (ended and process.poll() is not None):
                    if time.monotonic()-step.started>=timeout:
                        event(logger,'command_timeout','El comando excedió el tiempo máximo; se detendrá su árbol de procesos.',
                            level=logging.ERROR,stage=label,artifact=str(log_path),expected=expected_failure)
                        raise subprocess.TimeoutExpired(label,timeout)
                    try:line=messages.get(timeout=.1)
                    except queue.Empty:continue
                    if line is None:ended=True;continue
                    clean=logger.pipeline_redactor(re.sub(r'\x1b\[[0-9;]*m','',line))
                    output.write(clean);output.flush();lines.append(clean)
                    # Solo hitos dbt; SQL detallado queda en el archivo del comando.
                    if re.search(r'\b(?:\d+ of \d+|Running with dbt|Registered adapter|Completed|Done\.|PASS=|Catalog written|Finished running|Connection test)',clean):
                        level=logging.WARNING if re.search(r'\b(?:WARN|ERROR|FAIL)\b',clean) else logging.INFO
                        event(logger,'command_progress',clean.strip(),level=level,stage=label,expected=expected_failure)
                code=process.wait()
            result=subprocess.CompletedProcess(command,code,stdout=''.join(lines),stderr='')
            if code:
                event(logger,'command_rejected' if expected_failure else 'command_failed',
                    'Comando rechazado como parte de una prueba controlada.' if expected_failure else 'El comando falló; no se autoriza promover resultados.',
                    level=logging.WARNING if expected_failure else logging.ERROR,stage=label,
                    returncode=code,artifact=str(log_path),expected=expected_failure,
                    action='Revisar command_*.log y run_results.json para identificar el modelo o prueba.')
                raise CommandFailed(label,result)
            return result
        except BaseException:
            if process is not None:
                try:
                    complete=_terminate_tree(process)
                    if not complete:
                        event(logger,'process_cleanup_warning','Se detuvo el proceso principal; no se confirmó la terminación de todos sus descendientes.',
                            level=logging.WARNING,stage=label,process_id=process.pid,
                            action='Revisar procesos hijos del comando antes de reintentar la publicación.')
                except Exception as cleanup_error:
                    event(logger,'process_cleanup_warning','No se confirmó la limpieza del comando; se conserva la excepción original.',
                        level=logging.WARNING,stage=label,error_type=type(cleanup_error).__name__,process_id=process.pid)
            raise
        finally:
            if reader:reader.join(timeout=2)
            if process is not None and process.stdout and (reader is None or not reader.is_alive()):process.stdout.close()
