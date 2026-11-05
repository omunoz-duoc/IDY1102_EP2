"""Cuatro data marts dependientes: el DWH integrado publica productos por área de negocio.

Cada mart tiene un esquema propio, una tabla resumen persistida (construida por dbt sobre una
instantánea candidata), vistas de hechos y dimensiones conformadas y una vista de indicadores.
Los objetos de este registro son identificadores fijos del proyecto, nunca entrada del usuario.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from ..observability import pipeline_step
from ..snowflake import expanded_select

_MARTS = {
    'dm_comercial': {
        'nombre': 'Comercial', 'tabla': 'ventas_mensuales',
        'grano': 'Año × mes × sucursal × canal',
        'hechos': ['fact_ventas', 'fact_devoluciones'],
        'dimensiones': ['dim_fecha', 'dim_producto', 'dim_sucursal', 'dim_cliente', 'dim_canal', 'dim_campania'],
        'uso': 'Venta neta común, ticket promedio, productos y recurrencia de clientes'},
    'dm_operaciones': {
        'nombre': 'Operaciones', 'tabla': 'inventario_actual',
        'grano': 'Último corte global × producto × sucursal',
        'hechos': ['fact_inventario', 'fact_reposicion'],
        'dimensiones': ['dim_fecha', 'dim_producto', 'dim_sucursal'],
        'uso': 'Stock actual, conteos, reposición y rotación con historia semanal'},
    'dm_marketing': {
        'nombre': 'Marketing y Digital', 'tabla': 'campanias_resumen',
        'grano': 'Una campaña, incluida la clave 0 sin atribución',
        'hechos': ['fact_marketing', 'fact_web'],
        'dimensiones': ['dim_fecha', 'dim_producto', 'dim_cliente', 'dim_campania'],
        'uso': 'Envíos, consentimiento, embudo web y contraste con ventas asociadas a cupón'},
    # Clientes: identidad estable × mes, con periodos sin compra y atributos SCD al corte.
    'dm_clientes': {
        'nombre': 'Clientes', 'tabla': 'clientes_mensuales',
        'grano': 'Cliente identificado (NK estable) × año × mes',
        'hechos': ['fact_ventas', 'fact_devoluciones'],
        'dimensiones': ['dim_fecha', 'dim_cliente', 'dim_canal', 'dim_producto'],
        'uso': 'Recurrencia, clientes activos, recencia y medidas RFM observadas por corte mensual'},
}

MART_DDL = {
    'dm_comercial': '''CREATE TABLE dm_comercial.ventas_mensuales(
    anio bigint NOT NULL, mes bigint NOT NULL,
    sucursal_key bigint NOT NULL REFERENCES dw.dim_sucursal(sucursal_key),
    canal_key bigint NOT NULL REFERENCES dw.dim_canal(canal_key),
    venta_sin_devoluciones_clp bigint NOT NULL, devoluciones_clp bigint NOT NULL,
    venta_neta_clp bigint NOT NULL, unidades_vendidas bigint NOT NULL,
    unidades_devueltas bigint NOT NULL, documentos bigint NOT NULL,
    CONSTRAINT pk_dm_comercial PRIMARY KEY(anio,mes,sucursal_key,canal_key),
    CONSTRAINT ck_dm_comercial_mes CHECK(mes BETWEEN 1 AND 12),
    CONSTRAINT ck_dm_comercial_neto CHECK(venta_neta_clp=venta_sin_devoluciones_clp-devoluciones_clp))''',
    'dm_operaciones': '''CREATE TABLE dm_operaciones.inventario_actual(
    fecha_key bigint NOT NULL REFERENCES dw.dim_fecha(fecha_key),
    producto_key bigint NOT NULL REFERENCES dw.dim_producto(producto_key),
    sucursal_key bigint NOT NULL REFERENCES dw.dim_sucursal(sucursal_key),
    stock_sistema bigint NOT NULL, stock_fisico float NULL,
    stock_minimo bigint NULL, punto_reposicion bigint NULL, unidades_en_transito bigint NULL,
    sin_conteo bit NOT NULL, conteo_antiguo bit NOT NULL,
    diferencia_stock float NULL, riesgo_reposicion bit NOT NULL,
    CONSTRAINT pk_dm_operaciones PRIMARY KEY(fecha_key,producto_key,sucursal_key),
    CONSTRAINT ck_dm_operaciones_stock CHECK(stock_sistema>=0))''',
    'dm_marketing': '''CREATE TABLE dm_marketing.campanias_resumen(
    campania_key bigint NOT NULL REFERENCES dw.dim_campania(campania_key),
    envios bigint NOT NULL, entregados bigint NOT NULL, abiertos bigint NOT NULL,
    clicks bigint NOT NULL, conversiones_entregadas bigint NOT NULL,
    sin_consentimiento bigint NOT NULL, monto_atribuido bigint NOT NULL,
    sesiones bigint NOT NULL, con_carrito bigint NOT NULL, con_checkout bigint NOT NULL,
    compra_reportada bigint NOT NULL, documentos_asociados bigint NOT NULL,
    venta_reconocida_clp bigint NOT NULL, devoluciones_clp bigint NOT NULL,
    venta_neta_asociada_clp bigint NOT NULL,
    CONSTRAINT pk_dm_marketing PRIMARY KEY(campania_key),
    CONSTRAINT ck_dm_marketing_conversion CHECK(conversiones_entregadas<=entregados),
    CONSTRAINT ck_dm_marketing_neto CHECK(venta_neta_asociada_clp=venta_reconocida_clp-devoluciones_clp))''',
    'dm_clientes': '''CREATE TABLE dm_clientes.clientes_mensuales(
    cliente_nk nvarchar(64) NOT NULL, anio bigint NOT NULL, mes bigint NOT NULL,
    fecha_corte_key bigint NOT NULL REFERENCES dw.dim_fecha(fecha_key),
    cliente_key_corte bigint NOT NULL REFERENCES dw.dim_cliente(cliente_key),
    sin_version_al_corte bit NOT NULL,
    compras bigint NOT NULL, devoluciones bigint NOT NULL,
    unidades_compradas bigint NOT NULL, unidades_devueltas bigint NOT NULL,
    venta_sin_devoluciones_clp bigint NOT NULL, devoluciones_clp bigint NOT NULL,
    venta_neta_clp bigint NOT NULL,
    primera_compra_mes_key bigint NULL REFERENCES dw.dim_fecha(fecha_key),
    ultima_compra_mes_key bigint NULL REFERENCES dw.dim_fecha(fecha_key),
    ultima_compra_hasta_corte_key bigint NULL REFERENCES dw.dim_fecha(fecha_key),
    canal_principal_mes_key bigint NOT NULL REFERENCES dw.dim_canal(canal_key),
    compras_acumuladas bigint NOT NULL, venta_neta_acumulada_clp bigint NOT NULL,
    frecuencia_12m bigint NOT NULL, monetario_12m_clp bigint NOT NULL,
    meses_observados_12m bigint NOT NULL, recencia_dias bigint NULL, activo_6m bit NOT NULL,
    CONSTRAINT pk_dm_clientes PRIMARY KEY(cliente_nk,anio,mes),
    CONSTRAINT ck_dm_clientes_mes CHECK(mes BETWEEN 1 AND 12),
    CONSTRAINT ck_dm_clientes_compras CHECK(compras>=0 AND devoluciones>=0),
    CONSTRAINT ck_dm_clientes_neto CHECK(venta_neta_clp=venta_sin_devoluciones_clp-devoluciones_clp),
    CONSTRAINT ck_dm_clientes_ventana CHECK(meses_observados_12m BETWEEN 1 AND 12),
    CONSTRAINT ck_dm_clientes_recencia CHECK(recencia_dias IS NULL OR recencia_dias>=0))''',
}

# Claves de grano de cada tabla persistida.
MART_GRAIN = {
    'dm_comercial.ventas_mensuales': ['anio', 'mes', 'sucursal_key', 'canal_key'],
    'dm_operaciones.inventario_actual': ['fecha_key', 'producto_key', 'sucursal_key'],
    'dm_marketing.campanias_resumen': ['campania_key'],
    'dm_clientes.clientes_mensuales': ['cliente_nk', 'anio', 'mes'],
}


def mart_specs() -> dict:
    """Registro de marts con las jerarquías snowflake expuestas donde existe su dimensión base."""
    marts = copy.deepcopy(_MARTS)
    for spec in marts.values():
        if 'dim_producto' in spec['dimensiones']:
            spec['dimensiones'] += ['dim_linea', 'dim_categoria', 'dim_subcategoria']
        if 'dim_sucursal' in spec['dimensiones']:
            spec['dimensiones'] += ['dim_pais', 'dim_region', 'dim_comuna']
    return marts


MARTS = mart_specs()


def ensure_customer_views(conn, cutoff_key: int):
    """Atributos del cliente a la fecha de cada corte, sin unir todas sus versiones SCD."""
    conn.execute(text('''CREATE OR ALTER VIEW dm_clientes.v_clientes_mensuales AS
        SELECT m.*,d.segmento,d.region,d.comuna,CASE WHEN m.compras=0 THEN 'SIN_COMPRAS_MES' ELSE c.canal END canal_principal_mes,
        CAST(1.0*m.venta_sin_devoluciones_clp/NULLIF(m.compras,0) AS decimal(28,2)) ticket_promedio_mes_clp,
        CAST(CASE WHEN m.compras_acumuladas>=2 THEN 1 ELSE 0 END AS bit) es_recurrente_observado
        FROM dm_clientes.clientes_mensuales m JOIN dw.dim_cliente d ON m.cliente_key_corte=d.cliente_key
        JOIN dw.dim_canal c ON m.canal_principal_mes_key=c.canal_key'''))
    conn.execute(text('''CREATE OR ALTER VIEW dm_clientes.v_estado_clientes AS
        SELECT * FROM dm_clientes.v_clientes_mensuales
        WHERE fecha_corte_key=(SELECT MAX(fecha_corte_key) FROM dm_clientes.clientes_mensuales)'''))
    conn.execute(text('''CREATE OR ALTER VIEW dm_clientes.v_rfm AS
        SELECT cliente_nk,fecha_corte_key,recencia_dias,frecuencia_12m,
        monetario_12m_clp,meses_observados_12m,activo_6m
        FROM dm_clientes.v_estado_clientes'''))
    # Se mide cobertura financiera; las compras anónimas nunca se cuentan como una persona.
    cutoff_key = int(cutoff_key)
    conn.execute(text(f'''CREATE OR ALTER VIEW dm_clientes.v_cobertura_identidad AS
        WITH movimientos AS (
        SELECT f.fecha_key,CASE WHEN f.cliente_key=0 THEN 0 ELSE 1 END identificado,
            SUM(f.monto_neto) venta,CAST(0 AS bigint) devoluciones,
            COUNT_BIG(DISTINCT f.documento_nk) compras
        FROM dw.fact_ventas f GROUP BY f.fecha_key,CASE WHEN f.cliente_key=0 THEN 0 ELSE 1 END
        UNION ALL SELECT f.fecha_key,CASE WHEN f.cliente_key=0 THEN 0 ELSE 1 END,0,
            SUM(f.monto_neto_devuelto),0
        FROM dw.fact_devoluciones f GROUP BY f.fecha_key,CASE WHEN f.cliente_key=0 THEN 0 ELSE 1 END)
        SELECT d.anio,d.mes,m.identificado,
            CASE WHEN m.fecha_key<={cutoff_key} THEN 1 ELSE 0 END dentro_cobertura_compras,
            SUM(m.compras) compras,
            SUM(m.venta) venta_sin_devoluciones_clp,SUM(m.devoluciones) devoluciones_clp,
            SUM(m.venta)-SUM(m.devoluciones) venta_neta_clp
        FROM movimientos m JOIN dw.dim_fecha d ON m.fecha_key=d.fecha_key
        GROUP BY d.anio,d.mes,m.identificado,CASE WHEN m.fecha_key<={cutoff_key} THEN 1 ELSE 0 END'''))


def validate_customer_mart(conn, cutoff_key: int):
    checks = [
        ('''SELECT (SELECT SUM(compras) FROM dm_clientes.clientes_mensuales)
        -(SELECT COUNT_BIG(DISTINCT documento_nk) FROM dw.fact_ventas WHERE cliente_key<>0 AND fecha_key<=:customer_cutoff_key)''',
         'compras identificadas'),
        ('''SELECT (SELECT SUM(venta_sin_devoluciones_clp) FROM dm_clientes.clientes_mensuales)
        -(SELECT SUM(monto_neto) FROM dw.fact_ventas WHERE cliente_key<>0 AND fecha_key<=:customer_cutoff_key)''',
         'ventas identificadas'),
        ('''SELECT (SELECT SUM(devoluciones_clp) FROM dm_clientes.clientes_mensuales)
        -(SELECT SUM(monto_neto_devuelto) FROM dw.fact_devoluciones WHERE cliente_key<>0 AND fecha_key<=:customer_cutoff_key)''',
         'devoluciones identificadas'),
        ('''SELECT COUNT_BIG(*) FROM dm_clientes.clientes_mensuales m
        JOIN dw.dim_cliente c ON m.cliente_key_corte=c.cliente_key
        WHERE m.cliente_key_corte<>0 AND m.cliente_nk<>c.cliente_nk''', 'versión de identidad al corte')]
    for sql, label in checks:
        params = {'customer_cutoff_key': int(cutoff_key)} if ':customer_cutoff_key' in sql else {}
        if conn.scalar(text(sql), params) != 0:
            raise ValueError('Data mart Clientes no concilia: ' + label)


def check_customer_fixture(conn, refresh_sql: str) -> pd.DataFrame:
    """Prueba el SQL de producción con dos versiones SCD, meses vacíos y una devolución tardía."""
    conn.execute(text('''SET NOCOUNT ON; CREATE TABLE #cliente_fixture(
        cliente_key bigint,cliente_nk nvarchar(64),valido_desde datetime,valido_hasta datetime);
        INSERT INTO #cliente_fixture VALUES
        (0,'DESCONOCIDO','19000101','21000101'),
        (101,'PRUEBA_SCD','19000101','20260515'),
        (102,'PRUEBA_SCD','20260515','21000101');
        CREATE TABLE #venta_fixture(cliente_key bigint,documento_nk nvarchar(30),
            fecha_key bigint,canal_key bigint,cantidad bigint,monto_neto bigint);
        INSERT INTO #venta_fixture VALUES
        (101,'A',20260510,1,1,100),(101,'A',20260510,1,1,200),
        (102,'B',20260520,2,1,50),(102,'C',20260701,2,1,40),
        (0,'ANONIMA',20260715,1,1,999);
        CREATE TABLE #devolucion_fixture(cliente_key bigint,fecha_key bigint,
            cantidad bigint,monto_neto_devuelto bigint);
        INSERT INTO #devolucion_fixture VALUES(102,20260805,1,25),(0,20260805,1,50),(102,20260910,1,7);
        SELECT TOP 0 * INTO #salida_fixture FROM dm_clientes.clientes_mensuales;'''))
    sql = refresh_sql
    for source, target in [('dw.dim_cliente', '#cliente_fixture'),
                           ('dw.fact_ventas', '#venta_fixture'),
                           ('dw.fact_devoluciones', '#devolucion_fixture'),
                           ('dm_clientes.clientes_mensuales', '#salida_fixture')]:
        sql = sql.replace(source, target)
    conn.execute(text(sql), {'customer_cutoff_key': 20260805})
    demo = pd.read_sql(text('SELECT * FROM #salida_fixture ORDER BY anio,mes'), conn)
    assert len(demo) == 4 and demo.cliente_nk.nunique() == 1
    assert demo.compras.tolist() == [2, 0, 1, 0]
    assert demo.venta_neta_clp.tolist() == [350, 0, 40, -25]
    assert demo.compras_acumuladas.tolist() == [2, 2, 3, 3]
    assert demo.frecuencia_12m.tolist() == [2, 2, 3, 3]
    assert demo.cliente_key_corte.eq(102).all()
    assert demo.canal_principal_mes_key.tolist() == [1, 0, 2, 0]
    assert demo.recencia_dias.tolist() == [11, 41, 30, 35]
    assert int(demo.iloc[-1].fecha_corte_key) == 20260805
    assert int(demo.iloc[-1].monetario_12m_clp) == 365
    assert demo.activo_6m.all()
    # Dejar únicamente medidas y versiones ficticias como evidencia; no se muestran personas reales.
    cols = ['anio', 'mes', 'compras', 'venta_neta_clp', 'cliente_key_corte',
            'compras_acumuladas', 'frecuencia_12m', 'recencia_dias']
    conn.execute(text('''DROP TABLE #salida_fixture; DROP TABLE #devolucion_fixture;
        DROP TABLE #venta_fixture; DROP TABLE #cliente_fixture; SET NOCOUNT OFF;'''))
    return demo[cols]


def ensure_datamarts(conn, log, cutoff_key: int):
    """Crea esquemas, tablas persistentes y vistas dependientes sin copiar las dimensiones."""
    with pipeline_step(log, 'marts.preparar_tablas_y_vistas'):
        for schema, spec in MARTS.items():
            conn.execute(text(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA {schema}')"))
            if not conn.scalar(text('SELECT OBJECT_ID(:name,\'U\')'), {'name': schema + '.' + spec['tabla']}):
                conn.execute(text(MART_DDL[schema]))
            for table in spec['dimensiones'] + spec['hechos']:
                # Vistas con las mismas claves e historia del DWH. Son vistas, no copias materiales.
                conn.execute(text(f'CREATE OR ALTER VIEW {schema}.{table} AS ' + expanded_select(table)))
        conn.execute(text('''CREATE OR ALTER VIEW dm_comercial.v_kpis_ventas AS
        SELECT m.*,s.sucursal,s.region,c.canal,
        CAST(1.0*m.venta_sin_devoluciones_clp/NULLIF(m.documentos,0) AS decimal(28,2)) ticket_promedio_clp
        FROM dm_comercial.ventas_mensuales m JOIN dm_comercial.dim_sucursal s ON m.sucursal_key=s.sucursal_key
        JOIN dw.dim_canal c ON m.canal_key=c.canal_key'''))
        conn.execute(text('''CREATE OR ALTER VIEW dm_operaciones.v_alertas_stock AS
        SELECT i.*,p.producto,p.categoria,s.sucursal,s.region
        FROM dm_operaciones.inventario_actual i JOIN dm_operaciones.dim_producto p ON i.producto_key=p.producto_key
        JOIN dm_operaciones.dim_sucursal s ON i.sucursal_key=s.sucursal_key
        WHERE i.riesgo_reposicion=1 OR i.sin_conteo=1 OR i.conteo_antiguo=1'''))
        conn.execute(text('''CREATE OR ALTER VIEW dm_marketing.v_kpis_campanias AS
        SELECT c.codigo_campania,c.nombre_campania,c.calidad_campania,c.presupuesto_clp,m.*,
        CAST(1.0*m.conversiones_entregadas/NULLIF(m.entregados,0) AS decimal(28,6)) tasa_conversion_reportada,
        CASE WHEN c.calidad_campania='OK' THEN m.monto_atribuido/NULLIF(c.presupuesto_clp,0) END retorno_atribuido
        FROM dm_marketing.campanias_resumen m JOIN dw.dim_campania c ON m.campania_key=c.campania_key'''))
        ensure_customer_views(conn, cutoff_key)
        conn.execute(text('''IF OBJECT_ID('ctl.datamart_publicaciones','U') IS NULL
        CREATE TABLE ctl.datamart_publicaciones(
        run_id varchar(40) NOT NULL,datamart varchar(40) NOT NULL,tabla varchar(60) NOT NULL,
        publicado_utc datetime2 NOT NULL,filas bigint NOT NULL,
        CONSTRAINT pk_datamart_publicaciones PRIMARY KEY(run_id,datamart))'''))


def clear_datamarts(conn):
    # Primero dependientes: sus FK se refieren a las dimensiones que el DWH recargará.
    for schema, spec in MARTS.items():
        conn.execute(text(f"IF OBJECT_ID('{schema}.{spec['tabla']}','U') IS NOT NULL DELETE FROM {schema}.{spec['tabla']}"))


def refresh_datamarts(conn, run_id, candidate_schema, log, cutoff_key: int):
    """Última etapa SQL de publish(): promueve candidatos dbt y concilia antes del commit."""
    with pipeline_step(log, 'marts.promover_y_conciliar', publication_id=str(run_id)):
        # Un documento pertenece a una sola campaña: condición para atribuir devoluciones sin fan-out.
        conflicts = conn.scalar(text('''SELECT COUNT_BIG(*) FROM
        (SELECT documento_nk FROM dw.fact_ventas GROUP BY documento_nk
         HAVING COUNT(DISTINCT campania_key)>1) x'''))
        if conflicts:
            raise ValueError('Documento con campañas conflictivas antes de publicar data marts')
        for schema, spec in MARTS.items():
            # Promover resultados ya materializados y probados por dbt, respetando el DDL final.
            with pipeline_step(log, 'marts.promover_tabla', object=schema + '.' + spec['tabla']) as _mart_step:
                columns = list(conn.execute(text(f'SELECT TOP 0 * FROM {schema}.{spec["tabla"]}')).keys())
                actual = list(conn.execute(text(f'SELECT TOP 0 * FROM [{candidate_schema}].[{spec["tabla"]}]')).keys())
                if columns != actual:
                    raise ValueError('Contrato de columnas dbt distinto del mart: ' + schema)
                cols = ','.join('[' + col + ']' for col in columns)
                conn.execute(text(f'INSERT INTO {schema}.{spec["tabla"]} ({cols}) SELECT {cols} FROM [{candidate_schema}].[{spec["tabla"]}]'))
                count = conn.scalar(text(f'SELECT COUNT_BIG(*) FROM {schema}.{spec["tabla"]}'))
                if not count:
                    raise ValueError(f'Data mart vacío: {schema}')
                conn.execute(text('''INSERT INTO ctl.datamart_publicaciones
            (run_id,datamart,tabla,publicado_utc,filas) VALUES(:id,:dm,:tabla,:ts,:n)'''),
                             {'id': run_id, 'dm': schema, 'tabla': spec['tabla'],
                              'ts': datetime.now(timezone.utc).replace(tzinfo=None), 'n': count})
                log.info('datamart run=%s schema=%s tabla=%s filas=%d', run_id, schema, spec['tabla'], count)
                _mart_step.progress(rows=int(count))
        checks_sql = [
            ('''SELECT (SELECT SUM(venta_sin_devoluciones_clp) FROM dm_comercial.ventas_mensuales)
             -(SELECT SUM(monto_neto) FROM dw.fact_ventas)''', 'Ventas comerciales'),
            ('''SELECT (SELECT SUM(devoluciones_clp) FROM dm_comercial.ventas_mensuales)
             -(SELECT SUM(monto_neto_devuelto) FROM dw.fact_devoluciones)''', 'Devoluciones comerciales'),
            ('''SELECT (SELECT COUNT_BIG(*) FROM dm_operaciones.inventario_actual)
             -(SELECT COUNT_BIG(*) FROM dw.fact_inventario WHERE fecha_key=(SELECT MAX(fecha_key) FROM dw.fact_inventario))''',
             'Corte de inventario'),
            ('''SELECT (SELECT SUM(envios) FROM dm_marketing.campanias_resumen)
             -(SELECT COUNT_BIG(*) FROM dw.fact_marketing)''', 'Envíos'),
            ('''SELECT (SELECT SUM(sesiones) FROM dm_marketing.campanias_resumen)
             -(SELECT COUNT_BIG(*) FROM dw.fact_web)''', 'Sesiones'),
            ('''SELECT (SELECT SUM(venta_neta_asociada_clp) FROM dm_marketing.campanias_resumen)
             -(SELECT SUM(venta_neta_clp) FROM dm_comercial.ventas_mensuales)''', 'Neto asociado a campañas')]
        for sql, label in checks_sql:
            if conn.scalar(text(sql)) != 0:
                raise ValueError(f'Data mart no concilia: {label}')
        validate_customer_mart(conn, cutoff_key)
