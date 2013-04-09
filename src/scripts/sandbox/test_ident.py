'''
Created on 8 Apr 2013

@author: alexis_e
'''

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
for year in range(2006,2010):
    table_in_one = store.select('survey_'+str(year))
    print "annee %s" %(year)
    for entity in ['foy','men','fam']: 
        enum = 'qui'+entity
        ident = 'id'+entity
        list_ident = table_in_one[ident]
        list_qui =   table_in_one.ix[table_in_one[enum]  == 0 ,ident] 
        diff1 = set(list_qui).symmetric_difference(list_ident)
        for id in diff1:
            print "id%s %i doesn't have a head" %(entity, id)

                


    
    
store.close()    
