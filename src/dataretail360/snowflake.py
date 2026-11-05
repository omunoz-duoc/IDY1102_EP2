"""Jerarquías inmutables del DWH: claves persistentes y normalización reversible.

Los nombres se identifican dentro de su padre. Un cambio crea un miembro nuevo;
no se reasigna un miembro existente, para conservar la clasificación SCD2 pasada.
"""
import pandas as pd

HIERARCHIES = {
    'dim_pais': ('pais_key', None, 'pais'),
    'dim_region': ('region_key', 'pais_key', 'region'),
    'dim_comuna': ('comuna_key', 'region_key', 'comuna'),
    'dim_linea': ('linea_key', None, 'linea'),
    'dim_categoria': ('categoria_key', 'linea_key', 'categoria'),
    'dim_subcategoria': ('subcategoria_key', 'categoria_key', 'subcategoria'),
}
DIMENSION_LINKS = {
    'dim_region': {'pais_key': 'dim_pais'},
    'dim_comuna': {'region_key': 'dim_region'},
    'dim_sucursal': {'comuna_key': 'dim_comuna'},
    'dim_categoria': {'linea_key': 'dim_linea'},
    'dim_subcategoria': {'categoria_key': 'dim_categoria'},
    'dim_producto': {'subcategoria_key': 'dim_subcategoria'},
}
REMOVED_ATTRIBUTES = {
    'dim_producto': ['linea', 'categoria', 'subcategoria'],
    'dim_sucursal': ['pais', 'region', 'comuna'],
}


def members(incoming, previous, key, parent, label):
    """Reutiliza claves y agrega miembros; nunca modifica un padre ni un nombre previo."""
    natural = ([parent] if parent else []) + [label]
    if incoming[natural].isna().any().any():
        raise ValueError('Jerarquía con atributos nulos: '+label)
    old = previous.copy()
    if old.empty:
        old = pd.DataFrame([{key: 0, **({parent: 0} if parent else {}), label: 'DESCONOCIDO'}])
    if old[key].isna().any() or old[key].duplicated().any() or old.duplicated(natural).any():
        raise ValueError('Diccionario jerárquico inconsistente: '+label)
    zero = old.loc[old[key].eq(0)]
    if len(zero)!=1 or zero[label].iloc[0]!='DESCONOCIDO' or (parent and int(zero[parent].iloc[0])!=0):
        raise ValueError('Miembro 0 inválido: '+label)
    existing = set(old[natural].itertuples(index=False,name=None))
    new = sorted(set(incoming[natural].itertuples(index=False,name=None))-existing)
    added = pd.DataFrame(new,columns=natural)
    added.insert(0,key,range(int(old[key].max())+1,int(old[key].max())+1+len(added)))
    result = (old.copy() if added.empty else pd.concat([old,added],ignore_index=True))[[key]+natural]
    result[label]=result[label].astype('str')
    result[key]=result[key].astype('int64')
    if parent: result[parent]=result[parent].astype('int64')
    return result


def normalize_dimensions(logical, previous=None):
    """Normaliza TODAS las versiones de producto y las sucursales; retorna padres antes de hijos."""
    previous = previous or {}
    catalogs = {}
    product = logical['dim_producto'].copy()
    store = logical['dim_sucursal'].copy()
    for table,(key,parent,label) in HIERARCHIES.items():
        frame = store if table in ('dim_pais','dim_region','dim_comuna') else product
        natural = ([parent] if parent else [])+[label]
        catalog = members(frame,previous.get(table,pd.DataFrame()),key,parent,label)
        catalogs[table] = catalog
        frame = frame.merge(catalog,on=natural,how='left',validate='many_to_one')
        if frame[key].isna().any(): raise ValueError('Miembro jerárquico no resuelto: '+table)
        if table in ('dim_pais','dim_region','dim_comuna'): store=frame
        else: product=frame
    product=product.drop(columns=REMOVED_ATTRIBUTES['dim_producto']+['linea_key','categoria_key'])
    store=store.drop(columns=REMOVED_ATTRIBUTES['dim_sucursal']+['pais_key','region_key'])
    physical={**catalogs,**{name:frame.copy() for name,frame in logical.items()}}
    physical['dim_producto']=product
    physical['dim_sucursal']=store
    validate_links(physical)
    return physical


def expand_dimension(name, frame, catalogs):
    """Reconstruye la vista lógica usada por SCD, sin persistir atributos redundantes."""
    if name not in REMOVED_ATTRIBUTES: return frame.copy()
    chain = ['dim_subcategoria','dim_categoria','dim_linea'] if name=='dim_producto' else ['dim_comuna','dim_region','dim_pais']
    result=frame.copy()
    for table in chain:
        key,parent,label=HIERARCHIES[table]
        result=result.merge(catalogs[table],on=key,how='left',validate='many_to_one')
        if result[label].isna().any(): raise ValueError('FK jerárquica sin padre: '+table)
        result=result.drop(columns=key)
    return result


def validate_links(physical):
    for table,links in DIMENSION_LINKS.items():
        for key,parent in links.items():
            if physical[table][key].isna().any() or not physical[table][key].isin(physical[parent][key]).all():
                raise ValueError('FK jerárquica inválida: '+table+'.'+key)


def expanded_select(name, schema='dw'):
    """Vista de consumo: los joins reconstruyen atributos desde las tablas normalizadas."""
    if name=='dim_producto':
        return f'''SELECT p.*,s.subcategoria,c.categoria,l.linea FROM {schema}.dim_producto p
            JOIN {schema}.dim_subcategoria s ON p.subcategoria_key=s.subcategoria_key
            JOIN {schema}.dim_categoria c ON s.categoria_key=c.categoria_key
            JOIN {schema}.dim_linea l ON c.linea_key=l.linea_key'''
    if name=='dim_sucursal':
        return f'''SELECT s.*,c.comuna,r.region,p.pais FROM {schema}.dim_sucursal s
            JOIN {schema}.dim_comuna c ON s.comuna_key=c.comuna_key
            JOIN {schema}.dim_region r ON c.region_key=r.region_key
            JOIN {schema}.dim_pais p ON r.pais_key=p.pais_key'''
    return f'SELECT * FROM {schema}.{name}'
