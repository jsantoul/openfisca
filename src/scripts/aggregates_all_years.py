# -*- coding:utf-8 -*-
#
# This file is part of OpenFisca.
# OpenFisca is a socio-fiscal microsimulation software
# Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul
# Licensed under the terms of the GVPLv3 or later license
# (see openfisca/__init__.py for details)

# Script to compute the aggregates for all the referenced years

from src.lib.simulation import SurveySimulation 
from src.plugins.survey.aggregates import Aggregates
from src.plugins.survey.aggregates3 import Aggregates3
from pandas import ExcelWriter, ExcelFile
import os
import pandas.rpy.common as com

country = 'france'
destination_dir = "c:/users/utilisateur/documents/"
fname_all = "aggregates_inflated_loyers.xlsx"
fname_all = os.path.join(destination_dir, fname_all)              


def get_loyer_inflator(year):
    
    xls = ExcelFile('../countries/france/data/sources/loyers.xlsx')
    df = xls.parse('data', na_values=['NA'])   
    irl_2006 = df[ (df['year'] == 2006) & (df['quarter'] == 1)]['irl']
#    print irl_2006
    irl = df[ (df['year'] == year) & (df['quarter'] == 1)]['irl']
#    print irl 
    return float(irl.values/irl_2006.values)

def build_aggregates():

    writer = None
    years = range(2006,2010)
    tot1 = 0 
    tot3 = 0
    for year in years:        
        yr = str(year)
#        fname = "Agg_%s.%s" %(str(yr), "xls")
        simu = SurveySimulation()
        simu.set_config(year = yr, country = country)
        simu.set_param()
        import time
        
        
        deb3 = time.clock()
        simu.set_survey(num_table=3)
        simu.compute()
        fin3  = time.clock()
        
#        agg3 = Aggregates3()
#        agg3.set_simulation(simu)
#        agg3.compute()       

#        for ent in ['ind','men','foy','fam']:
##            dir_name = 'T:/Myliam2/output OF/' + ent +'.csv'
##            simu.survey.table3[ent].to_csv(dir_name)
#            import pdb
#            pdb.set_trace()
##            com.convert_to_r_dataframe
#            
#            dir_name = 'T:/Myliam2/output OF/' + 'output_'  + ent +'.csv'
#            simu.outputs.table3[ent].to_csv(dir_name)

        
        deb1 = time.clock()
        simu.set_survey(num_table=1)
        simu.compute()
        fin1  = time.clock()        
#        
#        agg = Aggregates()
#        agg.set_simulation(simu)
#        agg.compute()

#        if writer is None:
#            writer = ExcelWriter(str(fname_all))
#        agg.aggr_frame.to_excel(writer, yr, index= False, header= True)
        del simu
#        del agg
        import gc
        gc.collect()
        tot1 += fin1 - deb1
        tot3 += fin3 - deb3
        print fin1 - deb1
        print fin3 - deb3
    print tot1, tot3, tot3- tot1
#    writer.save()


def diag_aggregates():
    
    years = ['2006', '2007', '2008', '2009']
    
    df_final = None
    for yr in years:
        xls = ExcelFile(fname_all)
        df = xls.parse(yr, hindex_col= True)
        
        cols = [u"Mesure",
                u"Dépense \n(millions d'€)", 
                u"Bénéficiaires \n(milliers)", 
                u"Dépenses \nréelles \n(millions d'€)", 
                u"Bénéficiaires \nréels \n(milliers)", 
                u"Diff. relative \nDépenses",
                u"Diff. relative \nBénéficiaires"]
        selected_cols = [u"Mesure", u"Diff. relative \nDépenses", u"Diff. relative \nBénéficiaires"]
        df = df[selected_cols]
        df['year'] = yr
        df['num'] = range(len(df.index))
        df = df.set_index(['num', u'Mesure', 'year'])
        if df_final is None:
            df_final = df
        else:  

            df_final = df_final.append(df, ignore_index=False)
    
#    DataFrame.groupby()
    df_final = df_final.sortlevel(0)
    print str(fname_all)[:-5]+'_diag.xlsx'
    writer = ExcelWriter(str(fname_all)[:-5]+'_diag.xlsx')
    df_final.to_excel(writer, sheet_name="diagnostics", float_format="%.2f")
    writer.save()


    


if __name__ == '__main__':


#    build_aggregates()
#    diag_aggregates()
#    test()

    build_aggregates()