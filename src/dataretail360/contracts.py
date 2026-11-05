"""Contrato de las 16 fuentes lógicas: columnas obligatorias, claves naturales y lector.

El contrato es independiente del formato físico: SQL, CSV y Excel se validan igual.
El tipado y la validación de valores ocurren después, en Silver.
"""

# Columnas obligatorias por conjunto lógico (DDL del caso + columnas de los archivos).
CONTRACT = {
    'pos_sucursales': [
        'id_sucursal', 'codigo_sucursal', 'nombre_sucursal', 'formato', 'direccion', 'comuna',
        'region', 'zona', 'superficie_m2', 'fecha_apertura', 'fecha_cierre', 'estado',
        '_origen', '_fecha_carga',
    ],
    'pos_productos': [
        'codigo_producto', 'descripcion', 'linea_negocio', 'categoria', 'subcategoria',
        'marca', 'codigo_barra', 'precio_lista', 'costo_unitario', 'estado', 'fecha_alta',
        'fecha_baja', '_origen', '_fecha_carga',
    ],
    'pos_ventas_cabecera': [
        'id_boleta', 'tipo_documento', 'nro_documento', 'codigo_sucursal', 'nro_caja',
        'fecha_hora', 'rut_cliente', 'medio_pago', 'codigo_cupon', 'monto_bruto',
        'descuento_total', 'monto_neto', 'monto_iva', 'monto_total', 'estado', 'id_vendedor',
        '_origen', '_fecha_carga',
    ],
    'pos_ventas_detalle': [
        'id_boleta', 'nro_linea', 'codigo_producto', 'cantidad', 'precio_unitario',
        'descuento', 'total_linea', '_origen', '_fecha_carga',
    ],
    'ecom_catalogo': [
        'sku_web', 'nombre_web', 'categoria_web', 'marca', 'vendedor', 'precio_web',
        'codigo_pos_ref', 'ean', 'activo_web', 'fecha_publicacion', '_origen', '_fecha_carga',
    ],
    'ecom_cuentas': [
        'id_cuenta', 'email', 'nombre_completo', 'rut', 'telefono', 'comuna', 'fecha_creacion',
        'ultimo_login', 'acepta_newsletter', '_origen', '_fecha_carga',
    ],
    'ecom_pedidos': [
        'id_pedido', 'id_cuenta', 'email_contacto', 'id_sesion', 'canal_venta',
        'fecha_pedido_utc', 'estado_pedido', 'medio_pago', 'tipo_entrega',
        'codigo_sucursal_retiro', 'comuna_despacho', 'codigo_cupon', 'subtotal', 'descuento',
        'costo_despacho', 'total_pagado', 'fecha_entrega', '_origen', '_fecha_carga',
    ],
    'ecom_pedidos_detalle': [
        'id_pedido', 'nro_linea', 'sku_web', 'cantidad', 'precio_unitario', 'descuento',
        'total_linea', '_origen', '_fecha_carga',
    ],
    'web_sesiones': [
        'id_sesion', 'id_cuenta', 'fecha_inicio_utc', 'dispositivo', 'fuente_trafico',
        'utm_campaign', 'paginas_vistas', 'duracion_seg', 'sku_principal_visto',
        'agrego_carrito', 'inicio_checkout', 'compro', '_origen', '_fecha_carga',
    ],
    'crm_clientes': [
        'id_cliente', 'rut', 'nombres', 'apellido_paterno', 'apellido_materno', 'email',
        'telefono', 'fecha_nacimiento', 'genero', 'comuna', 'region', 'segmento_crm',
        'canal_registro', 'fecha_registro', 'acepta_marketing', 'estado', '_origen',
        '_fecha_carga',
    ],
    'mkt_campanias': [
        'id_campania', 'codigo_campania', 'nombre_campania', 'tipo_campania', 'canal_objetivo',
        'categoria_foco', 'segmento_objetivo', 'descuento_pct', 'fecha_inicio', 'fecha_fin',
        'presupuesto_clp', 'responsable', '_origen', '_fecha_carga',
    ],
    'mkt_envios_campania': [
        'id_envio', 'id_campania', 'id_cliente', 'canal_envio', 'fecha_envio', 'entregado',
        'abierto', 'fecha_apertura', 'click', 'convertido', 'monto_atribuido', '_origen',
        '_fecha_carga',
    ],
    'inv_stock_semanal': [
        'fecha_corte', 'codigo_sucursal', 'codigo_producto', 'stock_sistema', 'stock_fisico',
        'stock_minimo', 'punto_reposicion', 'unidades_en_transito', 'fecha_ultimo_conteo',
        '_origen', '_fecha_carga',
    ],
    'inv_ordenes_reposicion': [
        'id_orden', 'codigo_sucursal', 'codigo_producto', 'fecha_solicitud', 'fecha_recepcion',
        'cantidad_solicitada', 'cantidad_recibida', 'estado', '_origen', '_fecha_carga',
    ],
    'devoluciones': [
        'id_devolucion', 'canal_origen', 'id_documento_origen', 'codigo_producto',
        'fecha_devolucion', 'cantidad', 'monto_devuelto', 'motivo',
        'codigo_sucursal_recepcion', 'estado', '_origen', '_fecha_carga',
    ],
    'legacy_reporte_ventas_xlsx': [
        'archivo_origen', 'hoja', 'fila_excel', 'fecha', 'sucursal', 'linea', 'unidades',
        'monto_venta', 'estado', 'responsable', 'observacion', '_origen', '_fecha_carga',
    ],
}

# Claves naturales de los conjuntos lógicos, independientemente del formato físico.
KEYS = {
    'pos_sucursales': ['codigo_sucursal'], 'pos_productos': ['codigo_producto'],
    'pos_ventas_cabecera': ['id_boleta'], 'pos_ventas_detalle': ['id_boleta', 'nro_linea'],
    'ecom_catalogo': ['sku_web'], 'ecom_cuentas': ['id_cuenta'],
    'ecom_pedidos': ['id_pedido'], 'ecom_pedidos_detalle': ['id_pedido', 'nro_linea'],
    'web_sesiones': ['id_sesion'], 'crm_clientes': ['id_cliente'],
    'mkt_campanias': ['id_campania'], 'mkt_envios_campania': ['id_envio'],
    'inv_stock_semanal': ['fecha_corte', 'codigo_sucursal', 'codigo_producto'],
    'inv_ordenes_reposicion': ['id_orden'], 'devoluciones': ['id_devolucion'],
    'legacy_reporte_ventas_xlsx': ['archivo_origen', 'hoja', 'fila_excel'],
}

# Esquemas del origen PostgreSQL.
RAW_SCHEMA = 'dr360_raw'
CONTROL_SCHEMA = 'dr360_control'
CONTROL_TABLES = ['config_generacion', 'definiciones_kpi_por_area', 'problemas_calidad_inyectados']

# Hoja del XLSX exportado (la columna de negocio `hoja` permanece intacta).
LEGACY_SHEET = 'legacy_reporte_ventas_xlsx_2026'

# Registro de adaptadores. Una fuente tiene un único lector; no hay fallback a SQL.
FILE_SOURCES = {
    'mkt_campanias': {'tipo_fuente': 'CSV', 'ubicacion': 'data/raw/marketing/campanias.csv',
                      'hoja_entrada': '', 'encoding': 'utf-8-sig', 'sep': ','},
    'legacy_reporte_ventas_xlsx': {'tipo_fuente': 'EXCEL',
                                   'ubicacion': 'data/raw/legacy/reportes_ventas.xlsx',
                                   'hoja_entrada': LEGACY_SHEET},
}
SOURCES = {name: FILE_SOURCES.get(name, {'tipo_fuente': 'POSTGRESQL',
           'ubicacion': f'{RAW_SCHEMA}.{name}', 'hoja_entrada': ''}) for name in KEYS}
SQL_SOURCES = [name for name, spec in SOURCES.items() if spec['tipo_fuente'] == 'POSTGRESQL']
