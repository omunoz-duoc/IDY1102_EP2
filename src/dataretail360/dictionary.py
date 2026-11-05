"""Diccionario semántico generado: una fila por columna del DWH y de los data marts.

Sirve como capa semántica para BI y para una futura integración NLQ (lenguaje natural):
descripción, sinónimos, agregación válida y regla de uso de cada columna.
"""
from __future__ import annotations

import pandas as pd

from .observability import pipeline_step
from .report import markdown_table

# (descripción, sinónimos, agregación, regla NLQ)
DESCRIPTIONS = {
    'monto_neto': ('Venta de mercadería sin IVA antes de devoluciones, pesos CLP', 'venta; ingreso', 'SUM', 'No volver a descontar descuento ni IVA.'),
    'monto_con_iva': ('Mercadería con IVA después del descuento de línea, CLP', 'importe bruto', 'SUM', 'No incluye despacho web.'),
    'monto_neto_devuelto': ('Importe aprobado a restar de venta, sin IVA, CLP', 'devolución; reembolso', 'SUM', 'Restar una sola vez, según fecha de devolución.'),
    'cantidad': ('Unidades del evento', 'unidades; volumen', 'SUM', 'Interpretar según la tabla: venta o devolución.'),
    'stock_sistema': ('Unidades registradas en el corte de inventario', 'existencias; stock', 'SUM por corte', 'Nunca sumar fechas de corte.'),
    'stock_fisico': ('Unidades contadas; nulo significa no informado', 'conteo físico', 'SUM por corte', 'No reemplazar nulos por cero.'),
    'stock_minimo': ('Umbral mínimo por producto y sucursal', 'mínimo', 'No sumar entre fechas', 'Comparar con stock del mismo corte.'),
    'punto_reposicion': ('Umbral operativo para solicitar reposición', 'reposición', 'No sumar entre fechas', 'No confundir con stock mínimo.'),
    'unidades_en_transito': ('Unidades pendientes de llegada al corte', 'en tránsito', 'SUM por corte', 'No duplicar con órdenes.'),
    'diferencia_stock': ('Conteo físico menos stock de sistema', 'diferencia de inventario', 'SUM por corte', 'Nulo si no existe conteo.'),
    'riesgo_reposicion': ('Stock de sistema menor o igual al mínimo', 'riesgo; bajo mínimo', 'SUM banderas', 'Es una alerta, no prueba de quiebre efectivo.'),
    'sin_conteo': ('No se informó conteo físico', 'incompleto', 'SUM banderas', 'No equivale a stock cero.'),
    'conteo_antiguo': ('Conteo ausente o mayor a 30 días', 'desactualizado', 'SUM banderas', 'Evaluado respecto del corte.'),
    'monto_atribuido': ('Importe atribuido por plataforma de marketing, CLP', 'atribución', 'SUM', 'No sumar a ventas ni interpretar como causalidad.'),
    'presupuesto_clp': ('Presupuesto de campaña en CLP', 'inversión', 'Una vez por campaña', 'No sumarlo después de unir todos sus envíos.'),
    'sin_consentimiento': ('Envío a registro sin consentimiento vigente informado', 'consentimiento', 'SUM banderas', 'La fuente no tiene historia de consentimiento.'),
    'embudo_inconsistente': ('Interacción posterior en envío no entregado', 'embudo inválido', 'SUM banderas', 'Excluir de tasas condicionadas a entrega.'),
    'entregado': ('Envío entregado según plataforma', 'entrega', 'SUM banderas', 'Denominador de conversión reportada.'),
    'abierto': ('Apertura reportada del envío', 'apertura', 'SUM banderas', 'No es cliente único.'),
    'click': ('Clic reportado del envío', 'clic', 'SUM banderas', 'No equivale a venta.'),
    'convertido': ('Conversión reportada del envío', 'conversión', 'SUM banderas', 'Validar entregado; no implica causalidad.'),
    'paginas_vistas': ('Páginas vistas en la sesión', 'vistas', 'SUM', 'Una sesión por fila.'),
    'duracion_seg': ('Duración de sesión en segundos', 'duración', 'SUM o AVG', 'Expresar unidades.'),
    'agrego_carrito': ('La sesión agregó un producto al carrito', 'carrito', 'SUM banderas', 'No es cantidad de productos.'),
    'inicio_checkout': ('La sesión inició checkout', 'checkout', 'SUM banderas', 'No sumar a sesiones.'),
    'compro': ('Compra reportada por analítica web', 'compra web', 'SUM banderas', 'No reemplaza los pedidos entregados.'),
    'cantidad_solicitada': ('Unidades solicitadas en la orden', 'pedido reposición', 'SUM', 'Grano orden.'),
    'cantidad_recibida': ('Unidades recibidas acumuladas de la orden', 'recepción', 'SUM', 'Snapshot acumulado; no sumar versiones de la orden.'),
    'es_actual': ('Versión dimensional vigente', 'actual', 'No aplica', 'Para historia unir por clave del hecho, no filtrar actual.'),
    'valido_desde': ('Inicio inclusivo de vigencia', 'desde', 'No aplica', 'Intervalo [desde,hasta).'),
    'valido_hasta': ('Fin exclusivo de vigencia', 'hasta', 'No aplica', '2100 representa fin abierto en el laboratorio.'),
    'historia_observada': ('Versión creada desde un cambio observado', 'historia', 'No aplica', 'False identifica línea base sin historia reconstruida.'),
    'zona_anterior': ('Zona previa retenida por SCD3', 'zona previa', 'No aplica', 'Conserva solo un cambio, no historia completa.'),
    '_src_row': ('Posición de la fila en su instantánea Bronze', 'linaje', 'No aplica', 'Resolver junto con _batch.'),
    '_batch': ('SHA256 de la instantánea fuente', 'lote', 'No aplica', 'Consultar manifiesto; no es fecha de negocio.'),
    'cabecera_row': ('Fila de cabecera en Bronze', 'linaje cabecera', 'No aplica', 'Explica el neto distribuido.'),
    'cabecera_batch': ('SHA256 de instantánea de cabecera', 'lote cabecera', 'No aplica', 'Resolver con manifiesto.'),
    'cliente_nk': ('Identificador seudonimizado de entidad resuelta', 'persona', 'COUNT DISTINCT', 'No contar versiones SCD como personas.'),
    'documento_nk': ('Identificador de documento prefijado por sistema', 'ticket; boleta; pedido', 'COUNT DISTINCT', 'No contar líneas para ticket promedio.'),
    'region': ('Región del rol de la dimensión', 'territorio', 'Agrupar', 'Distinguir región del cliente y de la sucursal.'),
    'comuna': ('Comuna del rol de la dimensión', 'localidad', 'Agrupar', 'No confundir residencia con lugar de venta.'),
    'categoria': ('Categoría homologada de producto', 'familia; rubro', 'Agrupar', 'Usar versión histórica enlazada al hecho.'),
    'linea': ('Línea de negocio', 'línea', 'Agrupar', 'WEB_SIN_HOMOLOGAR conserva incertidumbre.'),
    'segmento': ('Segmento CRM observado', 'tipo de cliente', 'Agrupar', 'SCD2; no inferir historia anterior.'),
    'acepta_marketing': ('Consentimiento vigente observado', 'consentimiento actual', 'No aplica', 'No reconstruye consentimiento histórico.'),
}

# Atributos descriptivos sin regla especial.
ATTRIBUTES = {
    'fecha': 'Fecha de calendario',
    'anio': 'Año calendario',
    'mes': 'Mes calendario 1–12',
    'dia': 'Día del mes',
    'trimestre': 'Trimestre calendario 1–4',
    'semana_iso': 'Semana ISO',
    'anio_iso': 'Año de la semana ISO',
    'producto': 'Descripción de producto',
    'marca': 'Marca comercial',
    'subcategoria': 'Subcategoría de producto',
    'sucursal': 'Nombre de sucursal',
    'pais': 'País del rol geográfico',
    'zona': 'Zona comercial actual',
    'canal': 'Canal de venta',
    'codigo_campania': 'Código homologado de campaña',
    'nombre_campania': 'Nombre de campaña',
    'tipo_campania': 'Tipo de campaña',
    'fecha_inicio': 'Inicio de campaña',
    'fecha_fin': 'Fin de campaña',
    'calidad_campania': 'Validez de periodo y presupuesto',
    'dispositivo': 'Dispositivo de la sesión',
    'fuente_trafico': 'Origen del tráfico web',
    'estado': 'Estado actual de la orden',
    'fuente': 'Sistema original de venta',
}

# Columnas de los data marts: (descripción, agregación, regla NLQ)
MART_FIELDS = {
    'venta_sin_devoluciones_clp': ('Venta reconocida sin IVA antes de devoluciones, en CLP', 'SUM', 'No volver a descontar IVA ni descuentos.'),
    'devoluciones_clp': ('Reembolsos aprobados y trazables, sin IVA, CLP', 'SUM', 'Se restan una sola vez de la venta.'),
    'venta_neta_clp': ('Venta reconocida menos devoluciones, CLP', 'SUM', 'Puede ser negativa en meses con solo devoluciones.'),
    'unidades_vendidas': ('Unidades vendidas reconocidas', 'SUM', 'No contiene unidades devueltas.'),
    'unidades_devueltas': ('Unidades devueltas aprobadas', 'SUM', 'Restar si se requiere volumen neto.'),
    'documentos': ('Documentos distintos reconocidos en el periodo, sucursal y canal', 'SUM en este grano', 'Un documento pertenece a una sola fecha, sucursal y canal; ver prueba de conciliación.'),
    'envios': ('Envíos registrados de la campaña', 'SUM', 'No representa clientes distintos.'),
    'entregados': ('Envíos entregados de la campaña', 'SUM', 'Denominador de conversión reportada.'),
    'abiertos': ('Envíos con apertura reportada', 'SUM', 'No equivale a clientes únicos.'),
    'clicks': ('Envíos con clic reportado', 'SUM', 'No equivale a ventas.'),
    'conversiones_entregadas': ('Envíos entregados con conversión reportada', 'SUM', 'No demuestra causalidad; excluir conversiones de envíos no entregados.'),
    'sin_consentimiento': ('Envíos sin consentimiento vigente observado', 'SUM', 'La fuente no ofrece historia del consentimiento.'),
    'sesiones': ('Sesiones web asociadas a la campaña por UTM', 'SUM', 'No se unen sesiones a cada línea de venta.'),
    'con_carrito': ('Sesiones que agregaron al carrito', 'SUM', 'Indicador reportado por analítica web.'),
    'con_checkout': ('Sesiones que iniciaron checkout', 'SUM', 'No es cantidad de pedidos.'),
    'compra_reportada': ('Sesiones con compra reportada', 'SUM', 'No sustituye pedidos entregados del mart Comercial.'),
    'documentos_asociados': ('Documentos reconocidos asociados a una campaña o al miembro 0', 'SUM', 'No presupone exposición causal a la campaña.'),
    'venta_reconocida_clp': ('Venta reconocida asociada al cupón de la campaña, sin IVA', 'SUM', 'Campaña 0 agrupa documentos sin atribución.'),
    'venta_neta_asociada_clp': ('Venta asociada menos sus devoluciones trazables, CLP', 'SUM', 'La atribución sigue el documento original; no es un efecto causal.'),
    'cliente_nk': ('Identidad resuelta y seudonimizada, estable entre versiones SCD', 'COUNT DISTINCT', 'Una fila por identidad y mes. No sumar clientes únicos entre meses; se excluye la clave desconocida.'),
    'cliente_key_corte': ('Versión SCD válida al final del día de corte mensual', 'No aplica', 'FK a dim_cliente; 0 si no hay versión observada al corte. No utilizar siempre la versión actual.'),
    'fecha_corte_key': ('Fecha explícita de evaluación mensual', 'Agrupar / filtrar', 'Fin de mes; el último periodo se corta en fecha_fin, cobertura fiable de compras del dataset.'),
    'sin_version_al_corte': ('No hay atributos SCD observados para esa identidad al corte', 'SUM por corte', 'No elimina la identidad ni reasigna sus importes; describe incertidumbre de atributos.'),
    'compras': ('Documentos de compra distintos del cliente en el mes', 'SUM', 'Contar documentos, no líneas; cero en meses sin compra.'),
    'devoluciones': ('Registros aprobados de devolución del cliente en el mes', 'SUM', 'No son compras ni necesariamente documentos completos.'),
    'unidades_compradas': ('Unidades reconocidas de compras del mes', 'SUM', 'Mantener separadas de las unidades devueltas.'),
    'primera_compra_mes_key': ('Primera compra observada en ese mes', 'MIN con control de nulos', 'Nulo si el mes no tiene compras; no representa adquisición real del cliente.'),
    'ultima_compra_mes_key': ('Última compra observada en ese mes', 'MAX', 'Nulo en meses sin compra.'),
    'ultima_compra_hasta_corte_key': ('Última compra observada hasta el corte', 'MAX por identidad', 'Permite calcular recencia aunque no haya compra en el mes actual.'),
    'canal_principal_mes_key': ('Canal con más documentos de compra del mes', 'No aplica', 'Desempate por importe y luego clave menor; 0 si no hay compras. Describe conducta observada.'),
    'compras_acumuladas': ('Documentos de compra observados desde el inicio de historia hasta el corte', 'SUM entre clientes de un mismo corte', 'No sumar distintos cortes del mismo cliente.'),
    'venta_neta_acumulada_clp': ('Venta neta observada acumulada al corte, CLP', 'SUM entre clientes de un mismo corte', 'No es valor de vida predicho ni margen; no sumar snapshots temporales.'),
    'frecuencia_12m': ('Compras en hasta 12 meses calendario terminados en el mes del corte', 'SUM entre clientes de un mismo corte', 'Incluye meses sin compra; no sumar ventanas superpuestas.'),
    'monetario_12m_clp': ('Venta neta en hasta 12 meses calendario al corte, CLP', 'SUM entre clientes de un mismo corte', 'RFM monetario observado; incluye devoluciones y puede ser negativo.'),
    'meses_observados_12m': ('Meses disponibles desde primera actividad dentro de la ventana', 'No aplica', 'Entre 1 y 12; distingue ventanas incompletas y no demuestra fecha real de adquisición.'),
    'recencia_dias': ('Días del corte a la última compra observada', 'AVG / distribución por corte', 'Nulo sin compras observadas; no usar la fecha del sistema.'),
    'activo_6m': ('Última compra dentro de los seis meses calendario previos al corte, inclusive', 'SUM por corte', 'El corte debe coincidir al comparar indicadores; no es el estado ACTIVO del CRM.'),
}

MART_ATTRIBUTES = {
    'anio': 'Año calendario del movimiento',
    'mes': 'Mes local de Chile del movimiento',
}

# Ratios derivados de las vistas de indicadores: (tabla, columna, descripción, fórmula, regla)
DERIVED = [
    ('dm_clientes.v_clientes_mensuales', 'ticket_promedio_mes_clp', 'Venta antes de devoluciones por compra del mes', 'SUM(venta_sin_devoluciones_clp)/SUM(compras)', 'Nulo si no hay compras; no promediar promedios.'),
    ('dm_clientes.v_clientes_mensuales', 'es_recurrente_observado', 'Al menos dos documentos de compra observados hasta el corte', 'SUM por corte', 'Comparar una sola fecha por identidad; la historia disponible puede ser parcial.'),
    ('dm_comercial.v_kpis_ventas', 'ticket_promedio_clp', 'Venta antes de devoluciones por documento', 'SUM(venta_sin_devoluciones_clp)/SUM(documentos)', 'No promediar tickets de grupos; nulo si no hay documentos.'),
    ('dm_marketing.v_kpis_campanias', 'tasa_conversion_reportada', 'Proporción de envíos entregados con conversión', 'SUM(conversiones_entregadas)/SUM(entregados)', 'No promediar tasas; no implica causalidad.'),
    ('dm_marketing.v_kpis_campanias', 'retorno_atribuido', 'Monto de plataforma respecto del presupuesto', 'monto_atribuido/presupuesto_clp', 'Presupuesto una vez por campaña válida; no es ROI de beneficio.'),
]


def dwh_dictionary(all_frames: dict) -> pd.DataFrame:
    rows = []
    for table, frame in all_frames.items():
        for col in frame:
            if col in DESCRIPTIONS:
                desc, syn, agg, note = DESCRIPTIONS[col]
            elif col.endswith('_key'):
                desc = 'Clave sustituta de ' + col.removesuffix('_key').replace('_', ' ')
                syn, agg = 'identificador', 'No aplica'
                note = 'Unir con la dimensión correspondiente; 0 es desconocido/no aplicable.'
            elif col.endswith('_nk'):
                desc = 'Clave natural del evento o entidad ' + col.removesuffix('_nk')
                syn, agg, note = 'identificador de origen', 'No aplica', 'Respetar el grano declarado.'
            else:
                desc = ATTRIBUTES[col]
                syn, agg, note = col.replace('_', ' '), 'Agrupar / filtrar', 'Atributo descriptivo; no sumar.'
            rows.append({'tabla': table, 'columna': col, 'tipo': str(frame[col].dtype), 'descripcion': desc,
                         'sinonimos': syn, 'agregacion': agg, 'regla_NLQ': note})
    return pd.DataFrame(rows)


def mart_dictionary(mart_frames: dict) -> pd.DataFrame:
    rows = []
    for table, frame in mart_frames.items():
        for col in frame:
            if col in MART_FIELDS:
                desc, agg, note = MART_FIELDS[col]
            elif col in DESCRIPTIONS:
                desc, _, agg, note = DESCRIPTIONS[col]
            elif col.endswith('_key'):
                desc, agg = 'Clave compartida de ' + col.removesuffix('_key'), 'No aplica'
                note = 'FK a dimensión conformada; no reemplazar la clave histórica por la versión actual.'
            else:
                desc = MART_ATTRIBUTES[col]
                agg, note = 'Agrupar', 'Interpretar conjuntamente año y mes.'
            rows.append({'tabla': table, 'columna': col, 'tipo': str(frame[col].dtype), 'descripcion': desc,
                         'sinonimos': col.replace('_', ' '), 'agregacion': agg, 'regla_NLQ': note})
    for table, col, desc, formula, note in DERIVED:
        rows.append({'tabla': table, 'columna': col, 'tipo': 'ratio derivado', 'descripcion': desc,
                     'sinonimos': col.replace('_', ' '), 'agregacion': formula, 'regla_NLQ': note})
    rows.append({'tabla': 'dm_clientes.v_cobertura_identidad', 'columna': 'dentro_cobertura_compras',
                 'tipo': 'bandera 0/1', 'descripcion': 'Evento dentro del periodo de compras cubierto por fecha_fin',
                 'sinonimos': 'cobertura temporal', 'agregacion': 'Agrupar / filtrar',
                 'regla_NLQ': 'Filtrar 1 para conciliar con clientes_mensuales; 0 conserva reembolsos posteriores, '
                              'sin inferir inactividad.'})
    return pd.DataFrame(rows)


def write_dictionaries(ctx, all_frames: dict, mart_frames: dict) -> pd.DataFrame:
    gold = ctx.paths.gold
    with pipeline_step(ctx.log, 'documentacion.diccionario_dwh'):
        dictionary = dwh_dictionary(all_frames)
        dictionary.to_parquet(gold / 'diccionario_semantico.parquet', index=False)
    with pipeline_step(ctx.log, 'documentacion.diccionario_marts'):
        marts = mart_dictionary(mart_frames)
        marts.to_parquet(gold / 'datamarts' / 'diccionario_semantico.parquet', index=False)
        dictionary = pd.concat([dictionary, marts], ignore_index=True)
        dictionary.to_parquet(gold / 'diccionario_semantico.parquet', index=False)
    ctx.report.section('7. Diccionario semántico')
    ctx.report.details('Todas las columnas del DWH y de los data marts', markdown_table(dictionary, max_rows=None))
    ctx.report.text('''**IA generativa: uso responsable.** «¿Cuál fue la venta neta mensual por canal?» se resuelve con
el mart Comercial; «¿Cuántos clientes activos hay?» usa `COUNT DISTINCT cliente_nk`, no versiones SCD. Una futura
integración NLQ debe usar vistas de solo lectura, límites de filas, permisos mínimos, validación humana del SQL y
este diccionario. No enviar RUT, correos ni credenciales a un modelo.''')
    return dictionary
