# -*- coding:utf-8 -*-

"""
Convert a OF individual survey in a three level survey
"""
from pandas import HDFStore # DataFrame
import numpy as np
import os
import pdb
import src.countries.france.model.data
from src import SRC_PATH
import gc

country = 'france'
filename = None
if filename is None:
    if country is not None:
        filename = os.path.join(SRC_PATH, 'countries', country, 'data', 'survey.h5')
filename3 = os.path.join(SRC_PATH, 'countries', country, 'data', 'survey3.h5')

store = HDFStore(filename)
output = HDFStore(filename3)
#faire un remove de output pour pouvoir ecraser 

available_years = sorted([int(x[-4:]) for x in  store.keys()])

def getattr_deep(obj, attr):
    """Recurses through an attribute chain to get the ultimate value."""
    return reduce(getattr, attr.split('.'), obj)

def from_one_to_three(table,entity):
    from src.lib.utils import of_import
    InputTable = of_import('model.data', 'InputTable', country)    
    vars = [x for x in dir(InputTable()) if x[0] !="_" and x not in ['columns','get_comment','get_title','to_string']]
    vars_entity = []
    for var in vars:  
        if var in table.columns:
            if getattr_deep(InputTable(), str(var) +'.entity') == entity:
                vars_entity.append(str(var))
    return vars_entity
  

for year in  ['2006']:  # available_years: 
    table_in_one = store.select('survey_'+str(year))
    for entity in ['ind','foy','men']: 
        key = 'survey_'+str(year) + '/'+str(entity)
        vars_entity = from_one_to_three(table_in_one,entity)        
        table_entity = table_in_one[vars_entity]
        print key
        output.put(key, table_entity)
    del table_in_one
    gc.collect()

store.close()
output.close()

    
    




# get columns used by the tax and benefit system




