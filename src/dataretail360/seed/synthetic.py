"""Dataset sintético pequeño, determinista y ficticio (nunca lee datos reales).

3 sucursales, 6 productos, 8 identidades ficticias, 64 documentos de venta (ene–sep 2026),
5 devoluciones (una posterior al corte), 36 cortes de inventario, campaña, sesiones, envíos y
reposición. Incluye compras anónimas, meses sin compras, envíos sin consentimiento y una fecha
Excel ambigua para ejercitar las reglas. Los correos usan `example.invalid`.
"""
from datetime import datetime
from pathlib import Path
import pandas as pd
from ..contracts import CONTRACT, FILE_SOURCES, LEGACY_SHEET

VERSION = 'demo-v1'
SHEET = LEGACY_SHEET
CUTOFF = '2026-09-30'


def frames():
    rows = {name: [] for name in CONTRACT}

    def add(table, **values):
        unknown = set(values) - set(CONTRACT[table])
        if unknown:
            raise ValueError(f'Columnas desconocidas en {table}: {unknown}')
        row = dict.fromkeys(CONTRACT[table], None)
        row.update(_origen=VERSION, _fecha_carga=datetime(2026, 10, 1))
        row.update(values)
        rows[table].append(row)

    for i in range(1, 4):
        add('pos_sucursales', id_sucursal=i, codigo_sucursal=f'S{i}', nombre_sucursal=f'Sucursal Demo {i}',
            formato='TIENDA', direccion=f'Calle Ficticia {i}', comuna='SANTIAGO', region='METROPOLITANA',
            zona='CENTRO', superficie_m2=100*i, fecha_apertura=datetime(2023, 1, 1), estado='ACTIVA')
    for i in range(1, 7):
        add('pos_productos', codigo_producto=f'P{i}', descripcion=f'Producto Demo {i}', linea_negocio='HOGAR',
            categoria='COCINA' if i < 4 else 'ORGANIZACION', subcategoria='ACCESORIOS', marca='DEMO',
            codigo_barra=f'99000000000{i:02d}', precio_lista=1190*i, costo_unitario=500*i,
            estado='ACTIVO', fecha_alta=datetime(2023, 1, 1))
        add('ecom_catalogo', sku_web=f'W{i}', nombre_web=f'Producto Web Demo {i}', categoria_web='HOGAR',
            marca='DEMO', vendedor='DEMO', precio_web=1190*i, codigo_pos_ref=f'P{i}',
            ean=f'99000000000{i:02d}', activo_web=True, fecha_publicacion=datetime(2023, 1, 1))
    for i in range(1, 9):
        # Números generados con DV válido para ejercitar la homologación, sin datos de personas.
        digits = str(90000000+i)
        check = 11-sum(int(n)*(2+j%6) for j,n in enumerate(reversed(digits)))%11
        rut = digits+'-'+('0' if check == 11 else 'K' if check == 10 else str(check))
        add('crm_clientes', id_cliente=i, rut=rut, nombres=f'Persona Ficticia {i}', apellido_paterno='DEMO',
            apellido_materno='SINTETICA', email=f'cliente{i}@example.invalid', telefono='',
            fecha_nacimiento=datetime(1990, 1, i), genero='NO_INFORMADO', comuna='SANTIAGO',
            region='METROPOLITANA', segmento_crm='DEMO', canal_registro='TIENDA',
            fecha_registro=datetime(2023, 1, 1), acepta_marketing=i != 8, estado='ACTIVO')
        add('ecom_cuentas', id_cuenta=i, email=f'cliente{i}@example.invalid', nombre_completo=f'Persona Ficticia {i}',
            rut=rut, telefono='', comuna='SANTIAGO', fecha_creacion=datetime(2023, 1, 1),
            ultimo_login=datetime(2026, 9, 20), acepta_newsletter=i != 8)
    add('mkt_campanias', id_campania=1, codigo_campania='DEMO2026', nombre_campania='Campaña sintética',
        tipo_campania='FIDELIZACION', canal_objetivo='OMNICANAL', categoria_foco='HOGAR', segmento_objetivo='DEMO',
        descuento_pct=0, fecha_inicio=datetime(2026, 1, 1), fecha_fin=datetime(2026, 9, 30),
        presupuesto_clp=100000, responsable='EQUIPO DEMO')
    for month in (1, 3, 6, 9):
        for i in range(1, 9):
            key=month*100+i
            p=(i-1)%6+1
            net=1000*p
            gross=1190*p
            when=datetime(2026, month, 10+i, 12)
            rut=rows['crm_clientes'][i-1]['rut']
            add('pos_ventas_cabecera', id_boleta=key, tipo_documento='BOLETA', nro_documento=str(key),
                codigo_sucursal=f'S{(i-1)%3+1}', nro_caja=1, fecha_hora=when, rut_cliente=rut if i != 8 else '',
                medio_pago='TARJETA', codigo_cupon='DEMO2026' if i%2 else '', monto_bruto=gross,
                descuento_total=0, monto_neto=net, monto_iva=gross-net, monto_total=gross, estado='EMITIDA', id_vendedor=1)
            add('pos_ventas_detalle', id_boleta=key, nro_linea=1, codigo_producto=f'P{p}', cantidad=1,
                precio_unitario=gross, descuento=0, total_linea=gross)
            add('web_sesiones', id_sesion=f'SES{key}', id_cuenta=i, fecha_inicio_utc=when, dispositivo='MOVIL',
                fuente_trafico='EMAIL', utm_campaign='DEMO2026', paginas_vistas=4, duracion_seg=120,
                sku_principal_visto=f'W{p}', agrego_carrito=True, inicio_checkout=True, compro=True)
            add('ecom_pedidos', id_pedido=key, id_cuenta=i, email_contacto=f'cliente{i}@example.invalid',
                id_sesion=f'SES{key}', canal_venta='WEB' if i%2 else 'APP', fecha_pedido_utc=when,
                estado_pedido='ENTREGADO', medio_pago='TARJETA', tipo_entrega='DESPACHO',
                codigo_sucursal_retiro='', comuna_despacho='SANTIAGO', codigo_cupon='DEMO2026',
                subtotal=gross, descuento=0, costo_despacho=1000, total_pagado=gross+1000, fecha_entrega=when)
            add('ecom_pedidos_detalle', id_pedido=key, nro_linea=1, sku_web=f'W{p}', cantidad=1,
                precio_unitario=gross, descuento=0, total_linea=gross)
            add('mkt_envios_campania', id_envio=key, id_campania=1, id_cliente=i, canal_envio='EMAIL',
                fecha_envio=when, entregado=True, abierto=True, fecha_apertura=when, click=True,
                convertido=True, monto_atribuido=net)
    for i, month in enumerate((1, 3, 6, 9), 1):
        add('devoluciones', id_devolucion=i, canal_origen='TIENDA', id_documento_origen=month*100+1,
            codigo_producto='P1', fecha_devolucion=datetime(2026, month, 25), cantidad=1,
            monto_devuelto=1190, motivo='DEMO', codigo_sucursal_recepcion='S1', estado='APROBADA')
    # Reembolso posterior al corte de compras: prueba la cobertura financiera separada.
    add('devoluciones', id_devolucion=5, canal_origen='ECOMMERCE', id_documento_origen=902,
        codigo_producto='P2', fecha_devolucion=datetime(2026, 10, 5), cantidad=1,
        monto_devuelto=2380, motivo='DEMO', codigo_sucursal_recepcion='S1', estado='APROBADA')
    for day in (20, 27):
        for s in range(1, 4):
            for p in range(1, 7):
                add('inv_stock_semanal', fecha_corte=datetime(2026, 9, day), codigo_sucursal=f'S{s}',
                    codigo_producto=f'P{p}', stock_sistema=p*2, stock_fisico=p*2-1, stock_minimo=5,
                    punto_reposicion=10, unidades_en_transito=2, fecha_ultimo_conteo=datetime(2026, 9, day))
    add('inv_ordenes_reposicion', id_orden=1, codigo_sucursal='S1', codigo_producto='P1',
        fecha_solicitud=datetime(2026, 9, 20), fecha_recepcion=datetime(2026, 9, 25),
        cantidad_solicitada=10, cantidad_recibida=10, estado='RECIBIDA')
    add('legacy_reporte_ventas_xlsx', archivo_origen='demo.xlsx', hoja='demo', fila_excel=2,
        fecha='2026-09-11', sucursal='Sucursal Demo 1', linea='HOGAR', unidades=1,
        monto_venta='1000', estado='VALIDADO', responsable='DEMO', observacion='Dataset sintético')
    # Error deliberado de formato: ejercita rechazos sin romper la conciliación de ventas.
    add('legacy_reporte_ventas_xlsx', archivo_origen='demo.xlsx', hoja='demo', fila_excel=3,
        fecha='01/02/2026', sucursal='Sucursal Demo 1', linea='HOGAR', unidades=1,
        monto_venta='1000', estado='REVISAR', responsable='DEMO', observacion='Fecha ambigua inyectada')
    return {name: pd.DataFrame(values, columns=CONTRACT[name]) for name,values in rows.items()}


def write_files(root, dataset):
    """Escribe las dos fuentes de archivo en las rutas del contrato, relativas a `root`."""
    csv_path = Path(root) / FILE_SOURCES['mkt_campanias']['ubicacion']
    xlsx_path = Path(root) / FILE_SOURCES['legacy_reporte_ventas_xlsx']['ubicacion']
    for path in (csv_path, xlsx_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    dataset['mkt_campanias'].to_csv(csv_path, index=False, encoding='utf-8-sig')
    dataset['legacy_reporte_ventas_xlsx'].to_excel(xlsx_path, index=False, sheet_name=SHEET)


def controls():
    """Metadatos de `dr360_control`: corte de cobertura, KPI por área y problemas inyectados."""
    return {
        'config_generacion': pd.DataFrame([{'parametro': 'fecha_fin', 'valor': CUTOFF},
                                           {'parametro': 'dataset', 'valor': VERSION}]),
        'definiciones_kpi_por_area': pd.DataFrame([
            {'area': area, 'indicador': metric, 'definicion': definition} for area, metric, definition in [
                ('Comercial', 'Venta neta', 'Ventas netas menos devoluciones netas'),
                ('Operaciones', 'Stock', 'Último corte común'),
                ('Marketing', 'Conversión', 'Conversiones entregadas / envíos entregados'),
                ('Clientes', 'Actividad', 'Compras en seis meses calendario')]]),
        'problemas_calidad_inyectados': pd.DataFrame([
            {'tabla': 'legacy_reporte_ventas_xlsx', 'problema': 'Fecha ambigua', 'filas': 1},
            {'tabla': 'mkt_envios_campania', 'problema': 'Sin consentimiento', 'filas': 4}])}
