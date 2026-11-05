import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from dataretail360.snowflake import normalize_dimensions,expand_dimension,validate_links,HIERARCHIES


class SnowflakeTest(unittest.TestCase):
    def setUp(self):
        self.logical={
            'dim_producto':pd.DataFrame([
                {'producto_key':0,'producto_nk':'DESCONOCIDO','linea':'DESCONOCIDO','categoria':'DESCONOCIDO','subcategoria':'DESCONOCIDO'},
                {'producto_key':1,'producto_nk':'A','linea':'L1','categoria':'C','subcategoria':'S'},
                {'producto_key':2,'producto_nk':'B','linea':'L2','categoria':'C','subcategoria':'S'}]),
            'dim_sucursal':pd.DataFrame([
                {'sucursal_key':0,'pais':'DESCONOCIDO','region':'DESCONOCIDO','comuna':'DESCONOCIDO','zona':'DESCONOCIDO'},
                {'sucursal_key':1,'pais':'CHILE','region':'R1','comuna':'CENTRO','zona':'NORTE'},
                {'sucursal_key':2,'pais':'CHILE','region':'R2','comuna':'CENTRO','zona':'SUR'}])}

    def test_roundtrip_and_no_fanout(self):
        physical=normalize_dimensions(self.logical)
        for name in self.logical:
            restored=expand_dimension(name,physical[name],physical)
            assert_frame_equal(self.logical[name],restored[self.logical[name].columns],check_dtype=False)

    def test_homonyms_have_distinct_parents(self):
        p=normalize_dimensions(self.logical)
        self.assertEqual(len(p['dim_categoria'].query("categoria=='C'")),2)
        self.assertEqual(len(p['dim_comuna'].query("comuna=='CENTRO'")),2)

    def test_reclassification_preserves_old_path_and_keys(self):
        first=normalize_dimensions(self.logical)
        changed={k:v.copy() for k,v in self.logical.items()}
        old=changed['dim_producto'].iloc[[1]].assign(producto_key=3,linea='L3')
        changed['dim_producto']=pd.concat([changed['dim_producto'],old],ignore_index=True)
        second=normalize_dimensions(changed,first)
        for name in HIERARCHIES:
            key=HIERARCHIES[name][0]
            assert_frame_equal(first[name],second[name].loc[second[name][key].isin(first[name][key])].reset_index(drop=True))
        p=second['dim_producto'].set_index('producto_key')
        self.assertNotEqual(p.loc[1,'subcategoria_key'],p.loc[3,'subcategoria_key'])

    def test_repeat_and_unknown(self):
        first=normalize_dimensions(self.logical);second=normalize_dimensions(self.logical,first)
        for name in first: assert_frame_equal(first[name],second[name])
        for name,(key,parent,label) in HIERARCHIES.items():
            self.assertEqual(first[name].set_index(key).loc[0,label],'DESCONOCIDO')

    def test_orphan_rejected(self):
        p=normalize_dimensions(self.logical)
        p['dim_producto'].loc[0,'subcategoria_key']=99999
        with self.assertRaises(ValueError):validate_links(p)

    def test_duplicate_previous_member_rejected(self):
        p=normalize_dimensions(self.logical)
        p['dim_categoria']=pd.concat([p['dim_categoria'],p['dim_categoria'].iloc[[0]]],ignore_index=True)
        with self.assertRaises(ValueError):normalize_dimensions(self.logical,p)


if __name__=='__main__': unittest.main()
