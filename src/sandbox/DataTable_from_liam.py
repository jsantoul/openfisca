# -*- coding:utf-8 -*-

"""
Convert Liam output in OpenFisca Input
"""
from pandas import HDFStore, merge # DataFrame
import numpy as np
import pdb
import time
from src.lib.simulation import SurveySimulation 

input = "M:\Myliam2\Model\simulTest.h5"
input = "M:\Myliam2\Model\simulTest.h5"
output = "C:\Myliam2\Model\to_OF.h5"

name_convertion = {'person':'ind','declar':'foy','menage':'men', 'fam':'fam'}

store = HDFStore(input)
goal = HDFStore("M:/openfisca/src/countries/france/data/surveyLiam.h5")
#available_years = sorted([int(x[-4:]) for x in  store.keys()])


# on travaille d'abord sur l'ensemble des tables puis on selectionne chaque annee

# step 1
table = {}
nom = 'person'
base = 'entities/'+nom
ent = name_convertion[nom]
table[ent] = store[str(base)]
# get years
years = np.unique(table[ent]['period'].values)
# rename variables to make them OF ones
table[ent] = table[ent].rename(columns={'res': 'idmen', 'quires': 'quimen', 'foy': 'idfoy'})

# create fam base
table[ent][['idfam','quifam']] = table[ent][['idmen','quimen']]
# save information on qui == 0
foy0 = table[ent].ix[table[ent]['quifoy']==0,['id','idfoy','idmen','idfam','period']]
men0 = table[ent].ix[table[ent]['quimen']==0,['id','idfoy','idmen','idfam','period']]
fam0 = men0

for nom in ('menage','declar','fam'):
    ent = name_convertion[nom]    
    base = 'entities/'+nom
    ident = 'id'+ent
    if ent == 'fam':
        table[ent] = eval(ent +'0')
    else :
        table[ent] = store[str(base)].rename(columns={'id': ident})
        table[ent] = merge(table[ent], eval(ent +'0'), how='left', left_on=[ident,'period'], right_on=[ident,'period'])
                   
# test sur le nombre de qui ==0
for year in [2010]:
    for ent in ('men','foy','fam'):
        tab = table[ent].ix[table[ent]['period']==year,['id','id'+ent,'idfam']]
        ind = table['ind'].ix[table['ind']['period']==year,['qui'+ent]] 
        list_ind =  ind[ind==0]
        pdb.set_trace()   
            
for year in years:
    goal.remove('survey_'+str(year))
    for ent in ('ind','men','foy','fam'):
        tab = table[ent].ix[table[ent]['period']==year]
        key = 'survey_'+str(year) + '/'+ent     
        goal.put(key, tab) 
    if year == 2010:
        pdb.set_trace()
        len(tab['idfam'])
        len(np.unique(tab['idfam']))
        list_qui = tab['idfam']
        double = list_qui.value_counts()[list_qui.value_counts()>1]
        tabind = table['ind'].ix[table['ind']['period']==year]
        
        
store.close()
goal.close()

# on fais maintenant tourner le modèle OF
country = 'france'    
for year in years:        
    yr = str(year)
    deb3 =  time.clock()
    simu = SurveySimulation()
    simu.set_config(year = yr, country = country)
    simu.set_param()
    simu.set_survey(filename="M:/openfisca/src/countries/france/data/surveyLiam.h5", num_table=3)
    simu.compute()
    fin3  = time.clock()





