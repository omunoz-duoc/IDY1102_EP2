"""Calidad e integración Silver.

Cada regla produce evidencia. Se separan duplicados, conflictos y rechazos; no se pierde una
fila sin contabilizarla. La identidad y la homologación conservan incertidumbre cuando no hay
evidencia suficiente: RUT válido y correo único; similitud de nombres no basta para fusionar.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .contracts import KEYS
from .normalize import code_series, legacy_date, norm_series, region_series, rut_series
from .quality import QualityAuditor
from .report import Report

ECOM_STATES = ['ENTREGADO', 'DEVUELTO', 'CANCELADO', 'PAGO_RECHAZADO', 'EN_PREPARACION', 'EN_TRANSITO']


@dataclass
class Products:
    product_master: pd.DataFrame
    catalog: pd.DataFrame
    web_product_map: pd.Series


@dataclass
class Identities:
    crm: pd.DataFrame
    accounts: pd.DataFrame
    crm_map: pd.Series
    account_map: pd.Series
    rut_map: pd.Series
    client_master: pd.DataFrame
    ambiguous_emails: set


@dataclass
class SilverResult:
    tables: dict[str, pd.DataFrame]
    products: Products
    identities: Identities
    pos: pd.DataFrame
    orders: pd.DataFrame
    campaigns: pd.DataFrame
    campaign_map: pd.Series
    legacy: pd.DataFrame


def normalize_sources(raw: dict[str, pd.DataFrame], auditor: QualityAuditor) -> dict[str, pd.DataFrame]:
    """Homologa códigos y textos; separa claves nulas, duplicados exactos y claves conflictivas."""
    silver = {}
    for name, frame in raw.items():
        df = frame.copy()
        if name == 'mkt_campanias':
            # CSV llega como texto: códigos conservan su representación y medidas se tipan aquí.
            campaign_id = pd.to_numeric(df.id_campania, errors='coerce')
            df['id_campania'] = campaign_id.where(campaign_id.gt(0) & campaign_id.mod(1).eq(0)).astype('Int64')
            df['descuento_pct'] = pd.to_numeric(df.descuento_pct, errors='coerce')
            df['presupuesto_clp'] = pd.to_numeric(df.presupuesto_clp, errors='coerce').astype('float64')
        for col in ['codigo_producto', 'codigo_sucursal', 'codigo_sucursal_retiro',
                    'codigo_sucursal_recepcion', 'codigo_pos_ref', 'sku_web']:
            if col in df:
                df[col] = code_series(df[col])
        for col in ['estado', 'estado_pedido', 'canal_origen', 'canal_venta', 'tipo_entrega',
                    'linea_negocio', 'categoria', 'subcategoria']:
            if col in df:
                df[col] = norm_series(df[col])
        if 'region' in df:
            df['region'] = region_series(df.region)
        if 'comuna' in df:
            df['comuna'] = norm_series(df.comuna)
        for col in ['codigo_cupon', 'codigo_campania', 'utm_campaign']:
            if col in df:
                df[col] = code_series(df[col])
        required = KEYS[name]
        invalid_key = df[required].isna().any(axis=1)
        for col in required:
            if pd.api.types.is_string_dtype(df[col]):
                invalid_key |= df[col].eq('')
        silver[name] = auditor.audit(df, name, {'clave_nula': invalid_key}, required)
    return silver


def resolve_products(silver: dict[str, pd.DataFrame]) -> Products:
    """Maestro integrado de productos: EAN único prevalece sobre una referencia POS contradictoria."""
    products = silver['pos_productos'].copy()
    products['ean'] = products.codigo_barra.astype('string').str.strip()
    ean_unique = products.loc[products.ean.notna() & ~products.ean.duplicated(keep=False)].set_index('ean').codigo_producto
    catalog = silver['ecom_catalogo'].copy()
    catalog['ean'] = catalog.ean.astype('string').str.strip()
    catalog['por_ean'] = catalog.ean.map(ean_unique)
    valid_ref = catalog.codigo_pos_ref.isin(products.codigo_producto)
    catalog['referencia_contradictoria'] = catalog.por_ean.notna() & valid_ref & catalog.por_ean.ne(catalog.codigo_pos_ref)
    catalog['producto_nk'] = catalog.por_ean.fillna(catalog.codigo_pos_ref.where(valid_ref))
    catalog['metodo'] = np.select([catalog.por_ean.notna(), valid_ref], ['EAN_UNICO', 'REFERENCIA_POS'],
                                  default='WEB_INDEPENDIENTE')
    catalog['producto_nk'] = catalog.producto_nk.fillna('WEB:' + catalog.sku_web)
    web_product_map = catalog.set_index('sku_web').producto_nk
    products['producto_nk'] = products.codigo_producto
    products['producto'] = products.descripcion
    products['linea'] = products.linea_negocio
    products['subcategoria'] = products.subcategoria.replace('', 'SIN_INFORMAR')
    products['marca'] = norm_series(products.marca)
    extras = catalog.loc[catalog.metodo.eq('WEB_INDEPENDIENTE')].copy()
    extras = extras.assign(producto=extras.nombre_web, linea='WEB_SIN_HOMOLOGAR',
                           categoria=norm_series(extras.categoria_web), subcategoria='SIN_INFORMAR',
                           marca=norm_series(extras.marca))
    columns = ['producto_nk', 'producto', 'linea', 'categoria', 'subcategoria', 'marca']
    product_master = pd.concat([products[columns], extras[columns]], ignore_index=True)
    assert not product_master.producto_nk.duplicated().any()
    return Products(product_master=product_master, catalog=catalog, web_product_map=web_product_map)


def resolve_identities(silver: dict[str, pd.DataFrame]) -> Identities:
    """Identidades: RUT con DV válido; correo normalizado solo si no enlaza personas distintas."""
    crm = silver['crm_clientes'].copy()
    accounts = silver['ecom_cuentas'].copy()
    for df in [crm, accounts]:
        df['rut_ok'] = rut_series(df.rut)
        df['email_ok'] = df.email.astype('string').str.strip().str.lower()
        df['email_ok'] = df.email_ok.where(df.email_ok.str.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', na=False))
    identities = pd.concat([crm[['rut_ok', 'email_ok']], accounts[['rut_ok', 'email_ok']]], ignore_index=True)
    links = identities.dropna().drop_duplicates()
    email_counts = links.groupby('email_ok').rut_ok.nunique()
    email_rut = (links.loc[links.email_ok.isin(email_counts[email_counts.eq(1)].index)]
                 .drop_duplicates('email_ok').set_index('email_ok').rut_ok)
    ambiguous_emails = set(email_counts[email_counts.gt(1)].index)

    def identity(df, prefix, idcol):
        r = df.rut_ok.fillna(df.email_ok.map(email_rut))
        e = df.email_ok.where(~df.email_ok.isin(ambiguous_emails))
        k = ('R:' + r).fillna('E:' + e).fillna(prefix + df[idcol].astype(str))
        # Seudonimización determinista: no equivale a anonimización irreversible.
        return k.map(lambda x: hashlib.sha256(x.encode()).hexdigest())

    crm['cliente_nk'] = identity(crm, 'CRM:', 'id_cliente')
    accounts['cliente_nk'] = identity(accounts, 'WEB:', 'id_cuenta')
    crm_map = crm.set_index('id_cliente').cliente_nk
    account_map = accounts.set_index('id_cuenta').cliente_nk
    rut_map = (pd.concat([crm[['rut_ok', 'cliente_nk']], accounts[['rut_ok', 'cliente_nk']]])
               .dropna().drop_duplicates('rut_ok').set_index('rut_ok').cliente_nk)
    crm['segmento'] = norm_series(crm.segmento_crm)
    crm['prioridad'] = 0
    accounts = accounts.assign(region='SIN_INFORMAR', segmento='SOLO_WEB', prioridad=1,
                               acepta_marketing=accounts.acepta_newsletter)
    client_master = (pd.concat([crm, accounts], ignore_index=True)
                     .sort_values(['prioridad', 'cliente_nk']).drop_duplicates('cliente_nk'))
    client_master = client_master[['cliente_nk', 'comuna', 'region', 'segmento', 'acepta_marketing']].fillna(
        {'comuna': 'SIN_INFORMAR', 'region': 'SIN_INFORMAR', 'segmento': 'SIN_INFORMAR', 'acepta_marketing': False})
    assert not client_master.cliente_nk.duplicated().any()
    return Identities(crm=crm, accounts=accounts, crm_map=crm_map, account_map=account_map,
                      rut_map=rut_map, client_master=client_master, ambiguous_emails=ambiguous_emails)


def apply_document_rules(silver, auditor: QualityAuditor):
    """Fechas y reglas específicas: las fechas UTC digitales se convierten antes del corte diario."""
    pos = silver['pos_ventas_cabecera'].copy()
    pos['estado'] = pos.estado.replace({'NULA': 'ANULADA', 'ANUL': 'ANULADA'})
    pos['fecha_local'] = pd.to_datetime(pos.fecha_hora, errors='coerce')
    amounts = ['monto_neto', 'monto_iva', 'monto_total']
    pos = auditor.audit(pos, 'pos_cabecera_reglas', {
        'fecha_invalida': pos.fecha_local.isna(),
        'estado_desconocido': ~pos.estado.isin(['EMITIDA', 'ANULADA']),
        'montos_invalidos': pos[amounts].isna().any(axis=1) | pos[amounts].lt(0).any(axis=1),
        'cuadratura_tributaria': pos.monto_neto.add(pos.monto_iva).ne(pos.monto_total)})
    orders = silver['ecom_pedidos'].copy()
    orders['estado_pedido'] = orders.estado_pedido.replace({'DEVOLUCION': 'DEVUELTO'})
    orders['fecha_local'] = (pd.to_datetime(orders.fecha_pedido_utc, utc=True, errors='coerce')
                             .dt.tz_convert('America/Santiago').dt.tz_localize(None))
    amounts = ['subtotal', 'descuento', 'costo_despacho', 'total_pagado']
    orders = auditor.audit(orders, 'ecom_cabecera_reglas', {
        'fecha_invalida': orders.fecha_local.isna(),
        'estado_desconocido': ~orders.estado_pedido.isin(ECOM_STATES),
        'montos_invalidos': orders[amounts].isna().any(axis=1) | orders[amounts].lt(0).any(axis=1)
                            | orders.descuento.gt(orders.subtotal),
        'cuadratura_pedido': orders.subtotal.sub(orders.descuento).add(orders.costo_despacho).ne(orders.total_pagado)})
    return pos, orders


def prepare_campaigns(silver):
    campaigns = silver['mkt_campanias'].copy()
    campaigns['fecha_inicio'] = pd.to_datetime(campaigns.fecha_inicio, errors='coerce')
    campaigns['fecha_fin'] = pd.to_datetime(campaigns.fecha_fin, errors='coerce')
    campaigns['calidad_campania'] = np.where(
        campaigns.fecha_inicio.isna() | campaigns.fecha_fin.isna()
        | campaigns.fecha_fin.lt(campaigns.fecha_inicio) | campaigns.presupuesto_clp.isna()
        | campaigns.presupuesto_clp.le(0) | campaigns.descuento_pct.isna()
        | ~campaigns.descuento_pct.between(0, 100), 'REVISAR', 'OK')
    # Se conserva la campaña para integridad; las tasas de retorno no usan presupuestos inválidos.
    campaign_map = campaigns.set_index('codigo_campania').id_campania
    return campaigns, campaign_map


def clean_legacy(silver, auditor: QualityAuditor) -> pd.DataFrame:
    """Reportes manuales: tipado de fechas/montos y deduplicación por grano agregado."""
    legacy = silver['legacy_reporte_ventas_xlsx'].copy()
    legacy['fecha_limpia'] = legacy.fecha.map({x: legacy_date(x) for x in legacy.fecha.unique()})
    amount = (legacy.monto_venta.astype('string').str.replace(r'[$\s.]', '', regex=True)
              .str.replace(',', '.', regex=False))
    legacy['monto_limpio'] = pd.to_numeric(amount, errors='coerce')
    legacy = auditor.audit(legacy, 'legacy_formatos', {
        'fecha_ambigua_o_invalida': legacy.fecha_limpia.isna(),
        'monto_invalido': legacy.monto_limpio.isna() | legacy.monto_limpio.lt(0)})
    # La deduplicación entre archivos usa dimensiones y medidas, manteniendo el reporte separado.
    legacy['sucursal_norm'] = norm_series(legacy.sucursal)
    legacy['linea_norm'] = norm_series(legacy.linea)
    return auditor.audit(legacy, 'legacy_grano', {}, ['fecha_limpia', 'sucursal_norm', 'linea_norm'])


def build_silver(raw: dict[str, pd.DataFrame], auditor: QualityAuditor,
                 silver_dir: Path | None = None, control_dir: Path | None = None,
                 report: Report | None = None) -> SilverResult:
    """Silver completo. Sin `silver_dir` no escribe archivos (útil en pruebas)."""
    tables = normalize_sources(raw, auditor)
    products = resolve_products(tables)
    identities = resolve_identities(tables)
    pos, orders = apply_document_rules(tables, auditor)
    campaigns, campaign_map = prepare_campaigns(tables)
    legacy = clean_legacy(tables, auditor)
    if silver_dir is not None:
        products.catalog[['sku_web', 'producto_nk', 'metodo', 'referencia_contradictoria']].to_parquet(
            silver_dir / 'mapa_productos.parquet', index=False)
        legacy.to_parquet(silver_dir / 'legacy_conciliacion.parquet', index=False)
        for name, frame in tables.items():
            frame.to_parquet(silver_dir / f'{name}.parquet', index=False)
    if control_dir is not None:
        auditor.quality_frame().to_parquet(control_dir / 'calidad.parquet', index=False)
    if report is not None:
        report.section('2. Calidad e integración Silver')
        report.table(products.catalog.groupby(['metodo', 'referencia_contradictoria']).size()
                     .rename('productos').reset_index(), 'Homologación de productos web')
        report.text(f'Identidades consolidadas: **{len(identities.client_master)}**. '
                    f'Correos ambiguos: **{len(identities.ambiguous_emails)}**.')
        report.text('Documentos por estado (no todos son ventas reconocidas):')
        report.table(pos.groupby('estado').size().rename('documentos').reset_index())
        report.table(orders.groupby('estado_pedido').size().rename('documentos').reset_index())
        report.table(auditor.quality_frame(), 'Balance de calidad por etapa')
    return SilverResult(tables=tables, products=products, identities=identities, pos=pos, orders=orders,
                        campaigns=campaigns, campaign_map=campaign_map, legacy=legacy)
