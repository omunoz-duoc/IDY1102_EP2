"""Hechos y reglas del negocio.

Se resuelven claves después de preparar dimensiones. Los documentos reconocidos se reparten
entre sus líneas con conciliación exacta; devoluciones e inventario conservan su propio grano.

Contrato de indicadores:
- Venta neta POS: documento EMITIDA, `monto_neto` sin IVA, distribuido a líneas sin perder pesos.
- Venta neta e-commerce: ENTREGADO; mercadería (subtotal-descuento)/1,19, sin despacho, en CLP
  (19% es un supuesto del dataset).
- Devoluciones: APROBADA y trazable al documento/producto; se descuentan una vez en su fecha.
"""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .normalize import code_series, rut_series
from .observability import pipeline_step, report_data_warnings
from .quality import QualityAuditor
from .scd import temporal_lookup

FACT_COLUMNS = {
    'fact_ventas': ['venta_nk', 'documento_nk', 'fecha_key', 'producto_key', 'sucursal_key', 'cliente_key',
                    'canal_key', 'campania_key', 'cantidad', 'monto_neto', 'monto_con_iva', 'fuente',
                    '_src_row', '_batch', 'cabecera_row', 'cabecera_batch'],
}


@dataclass
class Facts:
    tables: dict[str, pd.DataFrame]
    sales: pd.DataFrame


def date_key(dates):
    return pd.to_datetime(dates).dt.strftime('%Y%m%d').astype('int64')


def money_round(values):
    """Neto sin IVA (19%) redondeado a peso entero, mitad hacia arriba."""
    return np.floor(pd.to_numeric(values) * 100 / 119 + 0.5).astype('int64')


def allocate_integer(frame, group, weight, target):
    """Método del mayor resto: reparte pesos enteros sin perder la cuadratura."""
    den = frame.groupby(group)[weight].transform('sum').astype('int64')
    if den.le(0).any():
        raise ValueError('Documento sin base positiva')
    numer = frame[weight].astype('int64') * frame[target].astype('int64')
    base = numer // den
    fractions = numer % den
    missing = frame[target] - base.groupby(frame[group]).transform('sum')
    rank = fractions.groupby(frame[group]).rank(method='first', ascending=False)
    return (base + rank.le(missing).astype('int64')).astype('int64')


class FactBuilder:
    def __init__(self, silver, dims, auditor: QualityAuditor):
        self.silver = silver
        self.dims = dims.logical
        self.audit = auditor.audit
        self.canal_map = self.dims['dim_canal'].set_index('canal').canal_key
        self.store_map = self.dims['dim_sucursal'].query('es_actual == True').set_index('sucursal_nk').sucursal_key

    def dimension_keys(self, frame, product=True, client=True, store=True):
        result = frame.copy()
        result['fecha_key'] = date_key(result.fecha_local)
        if product:
            result['producto_key'] = temporal_lookup(result, result.producto_nk, result.fecha_local,
                                                     self.dims['dim_producto'], 'producto_nk', 'producto_key')
        if client:
            result['cliente_key'] = temporal_lookup(result, result.cliente_nk, result.fecha_local,
                                                    self.dims['dim_cliente'], 'cliente_nk', 'cliente_key')
        if store:
            result['sucursal_key'] = result.sucursal_nk.map(self.store_map).fillna(0).astype('int64')
        return result

    def build_sales(self, detail, header, source, idcol, statuscol, validstatus):
        """Conservar identificadores del origen y validar documentos completos."""
        s, ids = self.silver, self.silver.identities
        d = detail.copy()
        h = header.copy()
        amounts = ['precio_unitario', 'descuento', 'total_linea']
        d = self.audit(d, source + '_detalle_reglas', {
            'cantidad_invalida': d.cantidad.isna() | d.cantidad.le(0),
            'importe_invalido': d[amounts].isna().any(axis=1) | d[amounts].lt(0).any(axis=1),
            'cuadratura_linea': d.cantidad.mul(d.precio_unitario).sub(d.descuento).ne(d.total_linea)})
        sums = d.groupby(idcol).total_linea.sum()
        h['suma_detalle'] = h[idcol].map(sums)
        expected = h.monto_total if source == 'POS' else h.subtotal - h.descuento
        h = self.audit(h, source + '_documento_completo', {
            'cabecera_detalle_no_cuadra': h.suma_detalle.isna() | h.suma_detalle.ne(expected),
            'importe_cero': expected.le(0)})
        h = h.loc[h[statuscol].eq(validstatus)].copy()
        if source == 'POS':
            h['objetivo_neto'] = h.monto_neto.astype('int64')
            h['cliente_nk'] = rut_series(h.rut_cliente).map(ids.rut_map)
            h['sucursal_nk'] = h.codigo_sucursal
            h['canal'] = 'TIENDA'
        else:
            h['objetivo_neto'] = money_round(h.subtotal - h.descuento)
            h['cliente_nk'] = h.id_cuenta.map(ids.account_map)
            h['sucursal_nk'] = h.codigo_sucursal_retiro.where(h.tipo_entrega.eq('RETIRO_TIENDA'), 'DIGITAL')
            h['canal'] = h.canal_venta
        h['campania_key'] = h.codigo_cupon.map(s.campaign_map).fillna(0).astype('int64')
        selected = [idcol, 'fecha_local', 'objetivo_neto', 'cliente_nk', 'sucursal_nk', 'canal', 'campania_key',
                    '_src_row', '_batch']
        h = h[selected].rename(columns={'_src_row': 'cabecera_row', '_batch': 'cabecera_batch'})
        d = self.audit(d, source + '_detalle_reconocido',
                       {'documento_no_reconocido_o_invalido': ~d[idcol].isin(h[idcol])})
        joined = d.merge(h, on=idcol, how='left', validate='many_to_one')
        joined['documento_nk'] = source + ':' + joined[idcol].astype(str)
        joined['venta_nk'] = joined.documento_nk + ':' + joined.nro_linea.astype(str)
        joined['producto_nk'] = (joined.codigo_producto if source == 'POS'
                                 else joined.sku_web.map(s.products.web_product_map))
        joined['monto_neto'] = allocate_integer(joined, 'documento_nk', 'total_linea', 'objetivo_neto')
        assert joined.groupby('documento_nk').monto_neto.sum().eq(
            joined.groupby('documento_nk').objetivo_neto.first()).all()
        joined['sistema'] = source
        return joined

    def sales(self):
        s = self.silver
        sales = pd.concat([
            self.build_sales(s.tables['pos_ventas_detalle'], s.pos, 'POS', 'id_boleta', 'estado', 'EMITIDA'),
            self.build_sales(s.tables['ecom_pedidos_detalle'], s.orders, 'ECOM', 'id_pedido', 'estado_pedido',
                             'ENTREGADO')], ignore_index=True)
        sales = self.dimension_keys(sales)
        sales['canal_key'] = sales.canal.map(self.canal_map).fillna(0).astype('int64')
        sales['monto_con_iva'] = sales.total_linea.astype('int64')
        sales['fuente'] = 'ventas_' + sales.sistema
        fact = sales[FACT_COLUMNS['fact_ventas']].copy()
        assert fact.venta_nk.is_unique
        return sales, fact

    def returns(self, sales):
        """Devoluciones: registro separado; solo descuentos trazables a ventas reconocidas."""
        s = self.silver
        returns = s.tables['devoluciones'].copy()
        returns['fecha_local'] = pd.to_datetime(returns.fecha_devolucion, errors='coerce')
        returns['documento_nk'] = (np.where(returns.canal_origen.eq('TIENDA'), 'POS:', 'ECOM:')
                                   + returns.id_documento_origen.astype(str))
        returns['producto_nk'] = returns.codigo_producto.where(
            returns.codigo_producto.isin(s.products.product_master.producto_nk),
            returns.codigo_producto.map(s.products.web_product_map))
        origin = (sales.groupby(['documento_nk', 'producto_nk'], dropna=False)
                  .agg(unidades_origen=('cantidad', 'sum'), bruto_origen=('monto_con_iva', 'sum'),
                       neto_origen=('monto_neto', 'sum'), cliente_nk=('cliente_nk', 'first'),
                       sucursal_venta=('sucursal_nk', 'first'), canal_key=('canal_key', 'first'),
                       fecha_venta=('fecha_local', 'min')).reset_index())
        returns = returns.merge(origin, on=['documento_nk', 'producto_nk'], how='left', validate='many_to_one')
        returns = returns.sort_values(['fecha_local', 'id_devolucion']).reset_index(drop=True)
        returns = self.audit(returns, 'devoluciones_validas', {
            'no_aprobada': returns.estado.ne('APROBADA'),
            'origen_no_reconocido': returns.unidades_origen.isna(),
            'fecha_invalida': returns.fecha_local.isna() | returns.fecha_local.lt(returns.fecha_venta.dt.normalize()),
            'importe_o_cantidad_invalida': returns.cantidad.isna() | returns.cantidad.le(0)
                                           | returns.monto_devuelto.isna() | returns.monto_devuelto.le(0),
            'canal_invalido': ~returns.canal_origen.isin(['TIENDA', 'ECOMMERCE'])})
        group = returns.groupby(['documento_nk', 'producto_nk'])
        returns = self.audit(returns, 'devoluciones_limites', {
            'excede_unidades_o_importe': group.cantidad.transform('sum').gt(returns.unidades_origen)
                                         | group.monto_devuelto.transform('sum').gt(returns.bruto_origen)})
        # Supuesto verificable: monto_devuelto está expresado con IVA, como el total de línea.
        # Se convierte usando la relación neto/bruto de la venta para respetar redondeos originales.
        returns['monto_neto_devuelto'] = np.floor(returns.monto_devuelto * returns.neto_origen
                                                  / returns.bruto_origen + 0.5).astype('int64')
        returns['sucursal_nk'] = returns.sucursal_venta
        returns = self.dimension_keys(returns)
        returns['canal_key'] = returns.canal_key.astype('int64')
        return returns[['id_devolucion', 'documento_nk', 'fecha_key', 'producto_key', 'cliente_key', 'sucursal_key',
                        'canal_key', 'cantidad', 'monto_neto_devuelto', '_src_row', '_batch']].rename(
            columns={'id_devolucion': 'devolucion_nk'})

    def inventory(self):
        """Inventario semanal: no se imputa un conteo físico ausente ni se suma stock entre semanas."""
        s = self.silver
        stock = s.tables['inv_stock_semanal'].copy()
        stock['fecha_local'] = pd.to_datetime(stock.fecha_corte, errors='coerce')
        stock['producto_nk'] = stock.codigo_producto
        stock['sucursal_nk'] = stock.codigo_sucursal
        stock = self.audit(stock, 'inventario_reglas', {
            'fecha_invalida': stock.fecha_local.isna(),
            'stock_sistema_invalido': stock.stock_sistema.isna() | stock.stock_sistema.lt(0),
            'maestro_desconocido': ~stock.producto_nk.isin(s.products.product_master.producto_nk)
                                   | ~stock.sucursal_nk.isin(self.store_map.index)})
        stock['sin_conteo'] = stock.stock_fisico.isna()
        stock['conteo_antiguo'] = ((stock.fecha_local - pd.to_datetime(stock.fecha_ultimo_conteo)).dt.days.gt(30)
                                   | stock.fecha_ultimo_conteo.isna())
        stock['diferencia_stock'] = stock.stock_fisico - stock.stock_sistema
        stock['riesgo_reposicion'] = stock.stock_sistema.le(stock.stock_minimo)
        stock = self.dimension_keys(stock, client=False)
        return stock[['fecha_key', 'producto_key', 'sucursal_key', 'stock_sistema', 'stock_fisico', 'stock_minimo',
                      'punto_reposicion', 'unidades_en_transito', 'sin_conteo', 'conteo_antiguo',
                      'diferencia_stock', 'riesgo_reposicion', '_src_row', '_batch']].copy()

    def marketing(self):
        """Marketing: conservar indicadores reportados y marcar problemas de consentimiento."""
        s, ids = self.silver, self.silver.identities
        marketing = s.tables['mkt_envios_campania'].copy()
        marketing['fecha_local'] = pd.to_datetime(marketing.fecha_envio, errors='coerce')
        marketing['cliente_nk'] = marketing.id_cliente.map(ids.crm_map)
        marketing['campania_key'] = marketing.id_campania.where(
            marketing.id_campania.isin(s.campaigns.id_campania), 0).astype('int64')
        consent = ids.crm.set_index('id_cliente').acepta_marketing
        marketing['sin_consentimiento'] = ~marketing.id_cliente.map(consent).fillna(False).astype(bool)
        marketing['embudo_inconsistente'] = ((marketing.abierto | marketing.click | marketing.convertido)
                                             & ~marketing.entregado)
        marketing = self.audit(marketing, 'marketing_reglas', {'fecha_invalida': marketing.fecha_local.isna(),
                                                                'monto_negativo': marketing.monto_atribuido.lt(0)})
        marketing = self.dimension_keys(marketing, product=False, store=False)
        return marketing[['id_envio', 'fecha_key', 'cliente_key', 'campania_key', 'entregado', 'abierto', 'click',
                          'convertido', 'monto_atribuido', 'sin_consentimiento', 'embudo_inconsistente',
                          '_src_row', '_batch']].rename(columns={'id_envio': 'envio_nk'})

    def web(self):
        """Navegación: una sesión es un hecho; no se replica una sesión por cada línea del pedido."""
        s, ids = self.silver, self.silver.identities
        web = s.tables['web_sesiones'].copy()
        web['fecha_local'] = (pd.to_datetime(web.fecha_inicio_utc, utc=True, errors='coerce')
                              .dt.tz_convert('America/Santiago').dt.tz_localize(None))
        web['cliente_nk'] = web.id_cuenta.map(ids.account_map)
        web['producto_nk'] = code_series(web.sku_principal_visto).map(s.products.web_product_map)
        web['campania_key'] = web.utm_campaign.map(s.campaign_map).fillna(0).astype('int64')
        web = self.audit(web, 'web_reglas', {'fecha_invalida': web.fecha_local.isna(),
                                             'metricas_invalidas': web.paginas_vistas.lt(0) | web.duracion_seg.lt(0)})
        web = self.dimension_keys(web, store=False)
        return web[['id_sesion', 'fecha_key', 'cliente_key', 'producto_key', 'campania_key', 'dispositivo',
                    'fuente_trafico', 'paginas_vistas', 'duracion_seg', 'agrego_carrito', 'inicio_checkout',
                    'compro', '_src_row', '_batch']].rename(columns={'id_sesion': 'sesion_nk'})

    def replenishment(self):
        """Reposición: foto acumulada del ciclo de una orden, con fecha de recepción opcional."""
        replen = self.silver.tables['inv_ordenes_reposicion'].copy()
        replen['fecha_local'] = pd.to_datetime(replen.fecha_solicitud, errors='coerce')
        replen['producto_nk'] = replen.codigo_producto
        replen['sucursal_nk'] = replen.codigo_sucursal
        replen = self.audit(replen, 'reposicion_reglas', {
            'fecha_invalida': replen.fecha_local.isna(),
            'cantidad_invalida': replen.cantidad_solicitada.le(0) | replen.cantidad_recibida.lt(0),
            'recepcion_antes_solicitud': pd.to_datetime(replen.fecha_recepcion).lt(replen.fecha_local)})
        replen = self.dimension_keys(replen, client=False)
        replen['fecha_recepcion_key'] = (pd.to_datetime(replen.fecha_recepcion).dt.strftime('%Y%m%d')
                                         .fillna('0').astype('int64'))
        return replen[['id_orden', 'fecha_key', 'fecha_recepcion_key', 'producto_key', 'sucursal_key',
                       'cantidad_solicitada', 'cantidad_recibida', 'estado', '_src_row', '_batch']].rename(
            columns={'id_orden': 'orden_nk'})


def _step(log, name, **fields):
    return pipeline_step(log, name, **fields) if log is not None else nullcontext()


def build_facts(silver, dims, auditor: QualityAuditor, log=None, control_dir: Path | None = None,
                report=None) -> Facts:
    builder = FactBuilder(silver, dims, auditor)
    facts = {}
    with _step(log, 'hechos.ventas'):
        sales, facts['fact_ventas'] = builder.sales()
    with _step(log, 'hechos.devoluciones_e_inventario'):
        facts['fact_devoluciones'] = builder.returns(sales)
        facts['fact_inventario'] = builder.inventory()
    with _step(log, 'hechos.marketing_web_reposicion'):
        facts['fact_marketing'] = builder.marketing()
        facts['fact_web'] = builder.web()
        facts['fact_reposicion'] = builder.replenishment()
        # Evidencia final de calidad: cuadre por etapa, sin mezclar granos ni contar motivos como filas.
        quality_df = auditor.quality_frame()
        assert quality_df.leidas.eq(quality_df.aceptadas + quality_df.rechazadas + quality_df.duplicadas).all()
        if control_dir is not None:
            quality_df.to_parquet(control_dir / 'calidad.parquet', index=False)
            auditor.rejections_frame().to_parquet(control_dir / 'rechazos_linaje.parquet', index=False)
        if log is not None:
            report_data_warnings(log, facts, ambiguous_count=len(silver.identities.ambiguous_emails))
    if report is not None:
        report.section('4. Hechos y reglas del negocio')
        report.text(f'Ventas reconocidas: **{len(sales)}** líneas; **{sales.documento_nk.nunique()}** documentos.')
        report.table(quality_df, 'Balance de calidad final por etapa')
        report.table(pd.DataFrame([{'hecho': n, 'filas': len(f)} for n, f in facts.items()]), 'Hechos construidos')
    return Facts(tables=facts, sales=sales)
