# -*- coding:utf-8 -*-

"""
Convert Liam output in OpenFisca Input
"""
from pandas import HDFStore # DataFrame
import numpy as np
import pdb

input = "M:\Myliam2\Model\simulTest.h5"
input = "T:\Myliam2\Model\simulTest.h5"
output = "T:\Myliam2\Model\to_OF.h5"

name_convertion = {'person':'ind','declar':'foy','menage':'men'}

store = HDFStore(input)
goal = HDFStore("T:/openfisca/src/countries/france/data/survey.h5")
#available_years = sorted([int(x[-4:]) for x in  store.keys()])
base = 'entities/person'
table = store[str(base)]
years = np.unique(table['period'].values)

# table by year
table_nom_year = {}
for year in years:
    table_year = {}
    store.remove('survey_'+str(year))
    for nom in ('person','menage','declar'):
        base = 'entities/'+nom
        table_nom = store[str(base)] 
        to_keep = table_nom['period']==year
        table_year[nom] = table_nom[to_keep]
        nom_of = name_convertion[nom]
        key = 'survey_'+str(year) + '/'+str(nom_of)
        store.put(key, table_year[nom])
        
#pdb.set_trace()
#store.keys()
#goal.keys()
#store.select('survey_2009/declar')
#goal.select('survey_2009')
#store.close()
#
#
#
#
#
#tab = table_nom_year[2009]
#tabp = tab['person']
#tabp_ref = tabp['quires']==1
#tabp_res = tabp['res']
#tabp_res[tabp_ref]


    
    




# get columns used by the tax and benefit system




