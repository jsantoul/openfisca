# -*- coding:utf-8 -*-

'''
Created on 26 Mar 2013

@author: alexis_e
'''

from pandas import HDFStore # DataFrame
import numpy as np
import tables
import os
import pdb
import src.countries.france.model.data
from src import SRC_PATH
import gc

country = 'france'
filename = None
def view(filename):
    # TODO: faire les choses bien avec le filename quand on squra ou le mettre
    # dans DataTable probablement et du coup le filename est deja gere
    filename3 = os.path.join(SRC_PATH, 'countries', country, 'data', 'survey3.h5')
    to_view = HDFStore(filename3)
    filename_view = os.path.join(SRC_PATH, 'countries', country, 'data', 'survey3_view.h5')
    view = tables.openFile(filename_view, mode="w")
    pdb.set_trace()
    test = tables.openFile(filename3, mode="r")
    output_globals = view.createGroup("/", "globals","Globals")      
    
    for table in to_view.keys():
        table_in_np = np.array(to_view.select(table)) 
        view.put(table, table_in_np)    
        del table_in_np
        gc()
                      
    to_view.close()
    view.close()           
        
view('pouet')