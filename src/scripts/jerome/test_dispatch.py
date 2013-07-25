# -*- coding:utf-8 -*-
#
# This file is part of OpenFisca.
# OpenFisca is a socio-fiscal microsimulation software
# Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul
# Licensed under the terms of the GPL (version 3 or later) license
# (see openfisca/__init__.py for details)

'''
Created on 25 juil. 2013

@author: Jérôme SANTOUL
'''

from src.lib.simulation import ScenarioSimulation
from src.lib.simulation import SurveySimulation
from src.plugins.survey.aggregates import Aggregates
from datetime import datetime
from pandas import ExcelWriter, HDFStore
import os
from src.countries.france.data.erf.aggregates import build_erf_aggregates
import pandas as pd

try:
    import xlwt
    from src.countries.france.XL import XLtable
except:
    pass



# from src.scripts.validation.check_consistency_tests import ( check_inputs_enumcols,
#                                                               check_entities,
#                                                               check_weights)

country = 'france'
# destination_dir = "c:/users/utilisateur/documents/"
# fname_all = "aggregates_inflated_loyers.xlsx"
# fname_all = os.path.join(destination_dir, fname_all)              

from src import SRC_PATH


def survey_case(year = 2006): 
    yr = str(year)
#        fname = "Agg_%s.%s" %(str(yr), "xls")
    simulation = SurveySimulation()
    survey_filename = os.path.join(SRC_PATH, 'countries', country, 'data', 'sources', 'test.h5')
    simulation.set_config(year=yr, country=country, 
                          survey_filename=survey_filename)
    simulation.set_param()


#    Ignore this
#    inflator = get_loyer_inflator(year)
#    simulation.inflate_survey({'loyer' : inflator})

    simulation.compute()
    simul_out_df = simulation.output_table.table
    simul_in_df = simulation.input_table.table
    print simul_out_df.head(20).to_string()

    print simul_out_df['af_base'].head(40).to_string()
    print 'now propagatin'
    simulation.output_table.propagate_to_members2(varname='aah', entity='ind')
    print 'output display'
    print simul_out_df['af_base'].head(40).to_string()
#     print 'input vars'
#     print simul_in_df.columns
#     print 'output vars'
#     print simul_out_df.columns
        # à répartir
#     col = simulation.output_table.description.get_col('af_base')
#     from_ent = col.entity
#     print '    from_ent =', from_ent
#     
#     varname = 'af_base'
#     value = simulation.output_table.get_value(varname) # at individual level
#     print 'value = ', value
#     print 'len = ', len(value)
#     
#     if simulation.num_table == 1:
#         try:
#             enum = simulation.output_table.description.get_col('qui'+from_ent).enum
#         except:
#             enum = simulation.output_table._inputs.description.get_col('qui'+from_ent).enum
#         head = simulation.output_table.index[from_ent][0]['idxIndi']
#         for member in enum:      
#             print member
#             value_member = value[head] 
#             print simulation.output_table.index[from_ent][member[1]]
#             select_unit = simulation.output_table.index[from_ent][member[1]]['idxUnit']
#             print len(select_unit)
#             select_unit2 = simulation.output_table.index[from_ent][member[1]]['idxIndi']
#             print len(select_unit2)
#             
#             value_member = value_member[select_unit]
#             print len(value_member)
#             print len(simulation.output_table.get_value(varname, from_ent, opt = [member[1]]))
#             simulation.output_table.set_value(varname, value_member, from_ent, _option = member[1])
#     print simul_out_df.loc[:,['af', 'af_base']].head(30).to_string()
    
#     agregates = simulation.output_table.get_value('af_base', entity='ind', opt=None) # Il faut que ce soit le vecteur de toutes les valeurs
#     keys = func(agregates) 
# 
#         #On calcule les clés dans un autre vecteur
#         
# #         idx = self.index["ind"][_option] #On ventile tjrs sur les ind et il faut tout préciser
#         
#         # On modifie proprement la valeur des presta à l'intérieur des familles.
#     dispatched_values = keys*value_member
#         # On intègre les valeurs dans la datatable
#     simulation.output_table.set_value('af_base', value = dispatched_values, entity='ind')
#     simulation.output_table.set_value('af_base', value=0, entity='fam', _option=None)
        
        
#     check_inputs_enumcols(simulation)
    
# Compute aggregates
#     agg = Aggregates()
#     agg.set_simulation(simulation)
#     agg.compute()
#     df1 = agg.aggr_frame
#     print df1.columns
#     
#     print df1.to_string()
    
#    Saving aggregates
#    if writer is None:
#        writer = ExcelWriter(str(fname)
#    agg.aggr_frame.to_excel(writer, yr, index= False, header= True)



if __name__ == '__main__':
    survey_case()