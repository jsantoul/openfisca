# -*- coding:utf-8 -*-
# Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul

"""

"""

from __future__ import division
import numpy as np
from pandas import DataFrame, read_csv, HDFStore
from src.lib.utils import of_import
from src.lib.description import ModelDescription, Description
import pdb
import time

# TODO : supprimer les num_table qui ne servent plus
class DataTable3(object):
    """
    Construct a SystemSf object is a set of Prestation objects
        * title [string]
        * comment [string]: text shown on the top of the first data item
    """

    
        
    def __init__(self, model_description, survey_data = None, scenario = None, datesim = None,
                  country = None, num_table = 3):
        super(DataTable3, self).__init__()
        # Init instance attribute
        self.description = None
        self.scenario = None
        self._isPopulated = False
        self.col_names = []
        self.table3 = {'ind' : DataFrame(), 'foy' : DataFrame(), 
                       'men' : DataFrame(), 'fam' : DataFrame()  }
        self.index = {}
        self._nrows = 0
        self.num_table = num_table
        #TODO :est-ce le bon endroit ? 
        self.list_entities = ['ind','men','foy','fam']
        
        if datesim is None:
            raise Exception('InputTable: datesim should be provided')
        else:
            self.datesim = datesim 
            
        if country is None:
            raise Exception('InputTable: country should be provided')
        else:
            self.country = country
                    
        self.survey_year = None
        
        # Build the description attribute        
        if type(model_description) == type(ModelDescription):
            descr = model_description()
            self.description = Description(descr.columns)
        else:
            raise Exception("model_description should be an ModelDescription inherited class")

        self.col_names = self.description.col_names

        if (survey_data is not None) and (scenario is not None):
            raise Exception("should provide either survey_data or scenario but not both")
        elif survey_data is not None:
            self.populate_from_survey_data(survey_data)
        elif scenario is not None:
            self.scenario = scenario
            scenario.populate_datatable(self)
        


    def gen_index(self, entities):
        '''
        Genrates indexex for the relevant entities
        '''
        
        self.index = {'ind': {0: {'idxIndi':np.arange(self._nrows), 
                                  'idxUnit':np.arange(self._nrows)}, # Units stand for entities
                      'nb': self._nrows}}

        for entity in entities:
            enum = self.description.get_col('qui'+entity).enum
            try:
                idx = getattr(self.table3['ind'], 'id'+entity).values
                qui = getattr(self.table3['ind'], 'qui'+entity).values
                enum = self.description.get_col('qui'+entity).enum
            except:
                raise Exception('DataTable needs columns %s and %s to build index with entity %s' %
                          ('id' + entity, 'qui' + entity, entity))

            self.index[entity] = {}
            dct = self.index[entity]
            idxlist = np.unique(idx)
            dct['nb'] = len(idxlist)

            for full, person in enum:
                idxIndi = np.sort(np.squeeze((np.argwhere(qui == person))))
#                if (person == 0) and (dct['nb'] != len(idxIndi)):
#                    raise Exception('Invalid index for %s: There is %i %s and %i %s' %(entity, dct['nb'], entity, len(idxIndi), full))
                idxUnit = np.searchsorted(idxlist, idx[idxIndi])
                temp = {'idxIndi':idxIndi, 'idxUnit':idxUnit}
                dct.update({person: temp}) 

    
    def propagate_to_members(self, varname, var):
        """
        Set the variable var of all entity member to the value of the (head of) entity
        """
        print varname, var

    def populate_from_survey_data(self, fname, year = None):
        '''
        Populates a DataTable from survey data
        '''
        list_entities = self.list_entities 
        
        if self.country is None:
            raise Exception('DataTable: country key word variable must be set') 
               
        # imports country specific variables from country.__init__.py
        INDEX = of_import(None, 'ENTITIES_INDEX', self.country)
        WEIGHT = of_import(None, 'WEIGHT', self.country)
        WEIGHT_INI = of_import(None, 'WEIGHT_INI', self.country)

        
        if fname[-4:] == '.csv':
            if self.num_table == 1 : 
                with open(fname) as survey_data_file:
                    self.table = read_csv(survey_data_file)
            else : 
                raise Exception('For now, use three csv table is not allowed'
                                'although there is no major difficulty. Please,'
                                'feel free to code it')        

        elif fname[-3:] == '.h5':
            deb1 = time.clock()
            store = HDFStore(fname)
            available_years = (sorted([int(x[-8:-4]) for x in  store.keys()]))
            # note we have a repetition here in available_years but it doesn't matter
            print time.clock() - deb1   
            if year is None:
                if self.datesim is not None:
                    year_ds  = self.datesim.year
                else:
                    raise Exception('self.datesim or year should be defined') 
            else:
                year_ds = year
           
            yr = year_ds+0 # to avoid pointers problem
            while yr not in available_years and yr > available_years[0]:
                yr = yr - 1
            base_name = 'survey_'+ str(yr)
            if year_ds != yr:
                print 'Survey data for year %s not found. Using year %s' %(str(year_ds), str(yr))
            else:
                print 'Survey data for year %s found' %str(year_ds)

            if yr in available_years:
                self.survey_year = yr
            for ent in list_entities:
                deb1 = time.clock()
                self.table3[ent] = store[str(base_name)+'/'+ ent]  
                print time.clock() - deb1        
            store.close()
            
        self._nrows = self.table3['ind'].shape[0]
        missing_col = []

        var_entity ={}
        for ent in list_entities:
            var_entity[ent] = [x for x in self.description.columns.itervalues() if x.entity == ent]
            for col in var_entity[ent]:
                if not col.name in self.table3[ent]:
                    missing_col.append(col.name)
                    self.table3[ent][col.name] = col._default
                try:   
                    self.table3[ent][col.name] = self.table3[ent][col.name].astype(col._dtype)
                except:
                    raise Exception("Impossible de lire la variable suivante issue des données d'enquête :\n %s \n  " %col.name) 
        if ent == 'foy':
            self.table3[ent] = self.table3[ent].to_sparse(fill_value=0)   
                      
        if missing_col:
            message = "%i input variables missing\n" % len(missing_col)
            messagef = ""
            messageb = ""
            missing_col.sort()
            for var in missing_col:
                if var[0] == 'f':
                    messagef += '  - '+ var +'\n'
                elif var[0] == 'b':
                    messageb += '  - '+ var +'\n'
                else:
                    message += '  - '+ var +'\n'
            print Warning(message + messagef + messageb)
        
        

        for var in INDEX:
            if ('id' + var) in missing_col:
                raise Exception('Survey data needs variable %s' % ('id' + var))
            
            if ('qui' + var) in missing_col:
                raise Exception('Survey data needs variable %s' % ('qui' + var))

        
        self.gen_index(INDEX)
        self._isPopulated = True
        # Initialize default weights
        self.set_value(WEIGHT_INI, self.get_value(WEIGHT), 'ind')

        
#        # TODO: activate for debug
#        print self.table.get_dtype_counts()
#        
#        for col in self.table.columns:
#            if col not in self.description.col_names:
#                print 'removing : ',  col
#                del self.table[col]
#        
#        print self.table.get_dtype_counts()

    def get_value(self, varname, entity=None, opt = None, sum_ = False):
        '''
        Read the value in an array
        
        Parameters
        ----------
        entity : str, default None
                 if "ind" or None return every individual, else return individuals belongig to the entity
        opt : dict
             dict with the id of the person for which you want the value
            - if opt is None, returns the value for the person 0 (i.e. 'vous' for 'foy', 'chef' for 'fam', 'pref' for 'men' in the "france" case)
            - if opt is not None, return a dict with key 'person' and values for this person
        
        Returns
        -------
        sumout: array
        
        '''
        col = self.description.get_col(varname)
        dflt = col._default
        dtyp = col._dtype
        entity_dest = col.entity
        
        
        var = np.array(self.table3[entity_dest][varname].values, dtype = col._dtype)
        
        if opt is not None and entity is None : 
            raise Exception("Entity must be given when opt is given")
        
        if opt is not None:
            out = {}
            nb = self.index[entity]['nb']
            for person in opt:
                temp = np.ones(nb, dtype = dtyp)*dflt
                idx = self.index[entity][person]
                temp[idx['idxUnit']] = var[idx['idxIndi']]
                out[person] = temp
            if sum_ is False:
                return out
            else:
                sumout = 0
                for val in out.itervalues():
                    sumout += val
                return sumout           
        return var
    
    

    def set_value(self, varname, value, entity = None, opt=None):
        '''
        Sets the value of varname using index and opt
        '''
        if entity is None:
            entity = "ind"
            
        col = self.description.get_col(varname)
#        values = self.table3[col.entity][varname].values
        dtyp = col._dtype
        temp = np.array(value, dtype = dtyp)
#        var = np.array(values, dtype = dtyp)
        if opt is None:
            idx = self.index[entity][0]
        else:
            idx = self.index[entity][opt]
        print col.entity,varname, idx, opt
        
        

        if entity=='ind' : 
            self.table3[entity].ix[idx['idxIndi'], [varname]] = value
        else:
            self.table3[entity].ix[idx['idxUnit'], [varname]] = value
            
    def to_csv(self, fname):
        # TODO: 
        self.table.to_csv(fname)
                  
    def __str__(self):
        return self.table.__str__()

    def inflate(self, varname, inflator):
        self.table[varname] = inflator*self.table[varname]


class SystemSf3(DataTable3):
    def __init__(self, model_description, param, defaultParam = None, datesim = None, country = None):
        DataTable3.__init__(self, model_description, datesim = datesim, country = country)
        self._primitives = set()
        self._param = param
        self._default_param = defaultParam
        self._inputs = None
        self.index = None
        if datesim is not None:
            self.datesim = datesim
            
        self.reset()
        self.build()

    def get_primitives(self):
        """
        Return socio-fiscal system primitives, ie variable needed as inputs
        """
        return self._primitives

    def reset(self):
        """ 
        Sets all columns as not calculated 
        """
        for col in self.description.columns.itervalues():
            col._isCalculated = False
                
    def disable(self, disabled_prestations):
        """
        Sets some column as calculated so they are cot evaluated and keep their default value
        """
        if disabled_prestations is not None:
            for colname in disabled_prestations:
                self.description.columns[colname]._isCalculated = True
    
    def build(self):
        # Build the closest dependencies  
        for col in self.description.columns.itervalues():
            # Disable column if necessary
            col.set_enabled()
            if col._start:
                if col._start > self.datesim: col.set_disabled()
            if col._end:
                if col._end < self.datesim: col.set_disabled()

            for input_varname in col.inputs:
                if input_varname in self.description.col_names:
                    input_col = self.description.get_col(input_varname)
                    input_col.add_child(col)
                else:                    
                    self._primitives.add(input_varname)
        
    def set_inputs(self, inputs, country = None):
        """ 
        Set the input DataTable
        
        Parameters
        ----------
        inputs : DataTable, required
                 The input variable datatable
        country: str, default None
                 The country of the simulation. this information is used to preprocess the inputs                
        """
        if not isinstance(inputs, DataTable3):
            raise TypeError('inputs must be a DataTable')
        # check if all primitives are provided by the inputs
#        for prim in self._primitives:
#            if not prim in inputs.col_names:
#                raise Exception('%s is a required input and was not found in inputs' % prim)

        # store inputs and indexes and nrows
        self._inputs = inputs
        self.index = inputs.index
        self._nrows = inputs._nrows

        # initialize the pandas DataFrame to store data
        self.table3 = {'ind' : DataFrame(), 'foy' : DataFrame(), 
                       'men' : DataFrame(), 'fam' : DataFrame()  } 
        
        dct = {'ind' : {}, 'foy' : {}, 'men' : {}, 'fam' : {} } 
        for col in self.description.columns.itervalues():
            dflt = col._default
            dtyp = col._dtype
            dent = col.entity
            size = self.index[dent]['nb']
            dct[dent][col.name] = np.ones(size, dtyp)*dflt
        
        print self.list_entities   
#        pdb.set_trace()           
        for ent in self.list_entities:
            self.table3[ent] = DataFrame(dct[ent]) 

        
        # Preprocess the input data according to country specification
        if country is None:
            country = 'france'
        preproc_inputs = of_import('utils','preproc_inputs', country = country)
        if preproc_inputs is not None:
            preproc_inputs(self._inputs)
        

    def calculate(self, varname = None):
        '''
        Solver: finds dependencies and calculate accordingly all needed variables 
        
        Parameters
        ----------
        
        varname : string, default None
                  By default calculate all otherwise, calculate only one variable
        
        '''
        if varname is None:
            for col in self.description.columns.itervalues():
#                try:
                    self.calculate(col.name)
#                except Exception as e:
#                    print e
#                    print col.name
            return # Will calculate all and exit

        col = self.description.get_col(varname)

        if not self._primitives <= self._inputs.col_names:
            raise Exception('%s are not set, use set_inputs before calling calculate. Primitives needed: %s, Inputs: %s' % (self._primitives - self._inputs.col_names, self._primitives, self._inputs.col_names))

        if col._isCalculated:
            return
        
        if not col._enabled:
            return
        
#        idx = self.index[col._entity]

        ent = col._entity
        if ent is None:
            ent = "ind"

        required = set(col.inputs)
        funcArgs = {}
        for var in required:
            if var in self._inputs.col_names:
                if var in col._option: 
                    funcArgs[var] = self._inputs.get_value(var, ent, col._option[var])
                else:
                    funcArgs[var] = self._inputs.get_value(var)
        if varname == 'rev_cap_bar':
            pdb.set_trace()
        for var in col._parents:
            parentname = var.name
            if parentname in funcArgs:
                raise Exception('%s provided twice: %s was found in primitives and in parents' %  (varname, varname))
            self.calculate(parentname)
#            if varname == 'af_nbenf':
#                pdb.set_trace()
            if parentname in col._option:
                funcArgs[parentname] = self.get_value(parentname, ent, col._option[parentname])
            else:
                funcArgs[parentname] = self.get_value(parentname)
        
        if col._needParam:
            funcArgs['_P'] = self._param
            required.add('_P')
            
        if col._needDefaultParam:
            funcArgs['_defaultP'] = self._default_param
            required.add('_defaultP')
        provided = set(funcArgs.keys())        
        if provided != required:
            raise Exception('%s missing: %s needs %s but only %s were provided' % (str(list(required - provided)), self._name, str(list(required)), str(list(provided))))
        self.set_value(varname, col._func(**funcArgs), ent)
        col._isCalculated = True