# -*- coding:utf-8 -*-
# Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul

"""
openFisca, Logiciel libre de simulation du système socio-fiscal français
Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul

This file is part of openFisca.

    openFisca is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    openFisca is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with openFisca.  If not, see <http://www.gnu.org/licenses/>.
"""

from __future__ import division
import numpy as np
from src.lib.utils import Enum


def default_frequency_converter(from_ = None, to_= None):
    """
    Default function to convert Columns between different frequencies 
    """
    if (from_ is None) and (to_ is None):
        return lambda x: x

    if (from_ == "year") and (to_ == "trim"):
        return lambda x: x/4

    if (from_ == "trim") and (to_ == "year"):
        return lambda x: x*4

    if (from_ == "year") and (to_ == "month"):
        return lambda x: x/12 

    if (from_ == "month") and (to_ == "year"):
        return lambda x: 12*x
 


class Column(object):
    count = 0
    def __init__(self, label = None, default = 0, entity= 'ind', start = None, end = None, val_type = None, freq = 'year'):

        super(Column, self).__init__()
        self.name = None
        self.label = label
        self.entity = entity
        self.start = start
        self.end = end
        self.val_type = val_type
        self.freq = freq

        self._order = Column.count
        Column.count += 1
        self._default = default
        self._dtype = float
        self._frequency_converter = default_frequency_converter

        
    def reset_count(self):
        """
        Reset the count of column to zero
        """
        Column.count = 0
       
class IntCol(Column):
    '''
    A column of integer
    '''
    def __init__(self, **kwargs):
        super(IntCol, self).__init__(**kwargs)
        self._dtype = np.int32
        
class EnumCol(IntCol):
    '''
    A column of integer with an enum
    '''
    def __init__(self, enum = None, **kwargs):
        super(EnumCol, self).__init__(**kwargs)
        self._dtype = np.int16
        if isinstance(enum, Enum):
            self.enum = enum
        else:
            self.enum = None            
            
class BoolCol(Column):
    '''
    A column of boolean
    '''
    def __init__(self, default = False, **kwargs):
        super(BoolCol, self).__init__(default=default, **kwargs)
        self._dtype = np.bool

class FloatCol(Column):
    '''
    A column of float 32
    '''
    def __init__(self, **kwargs):
        super(FloatCol, self).__init__(**kwargs)
        self._dtype = np.float32
        
class AgesCol(IntCol):
    '''
    A column of Int to store ages of people
    '''
    def __init__(self, default=-9999, **kwargs):
        super(AgesCol, self).__init__(default=default, **kwargs)
        
class DateCol(Column):
    '''
    A column of Datetime 64 to store dates of people
    '''
    def __init__(self, val_type="date", **kwargs):
        super(DateCol, self).__init__(val_type=val_type, **kwargs)
        self._dtype = np.datetime64

class Prestation(Column):
    """
    Prestation is a wraper around a function which takes some arguments and return a single array. 
    _P is a reserved kwargs intended to pass a tree of parametres to the function
    """
    count = 0

    def __init__(self, func, entity= 'ind', label = None, start = None, end = None, val_type = None, freq="year"):
        super(Prestation, self).__init__(label=label, entity=entity, start=start, end=end, val_type=val_type, freq=freq)

        if func is None:
            raise Exception('a function to compute the prestation should be provided')
        
        self._order = Prestation.count
        Prestation.count += 1
        
        # initialize attribute
        self._isCalculated = False
        self._option = {}
        self._freq = {}
        self._func = func
        self._entity  = entity
        self._start = start
        self._end = end
        self._val_type = val_type
        self.entity  = entity

        self.inputs = set(func.__code__.co_varnames[:func.__code__.co_argcount])
        self._children  = set() # prestations immidiately affected by current prestation 
        self._parents = set() # prestations that current prestations depends on  
           
        # by default enable all the prestations 
        self._enabled    = True
       
        # check if the function func needs parameter tree _P
        self._needParam = '_P' in self.inputs
        if self._needParam:
            self.inputs.remove('_P')
            
        # check if the function func needs default parameter tree _P
        self._needDefaultParam = '_defaultP' in self.inputs
        if self._needDefaultParam:
            self.inputs.remove('_defaultP')
                
        # check if an option dict is passed to the function
        self._hasOption = '_option' in self.inputs
        if self._hasOption:
            self.inputs.remove('_option')
            self._option = func.func_defaults[0]
            for var in self._option:
                if var not in self.inputs:
                    raise Exception('%s in option but not in function args' % var)
                
        # check if a frequency dict is passed to the function
        self._has_freq = '_freq' in self.inputs
        if self._has_freq:
            self.inputs.remove('_freq')
            if self._hasOption:
                self._freq = func.func_defaults[1]
            else:
                self._freq = func.func_defaults[0]
                
            for var in self._freq:
                if var not in self.inputs:
                    raise Exception('%s in option but not in function args' % var)
                
    
    def set_enabled(self):
        self._enabled = True
 
    def set_disabled(self):
        self._enabled = False

    def add_child(self, prestation):
        self._children.add(prestation)
        prestation._parents.add(self)

class BoolPresta(Prestation, BoolCol):
    '''
    A Prestation inheriting from BoolCol
    '''
    def __init__(self, func, **kwargs):
        BoolCol.__init__(self, **kwargs)
        Prestation.__init__(self, func=func, **kwargs)

class IntPresta(Prestation, IntCol):
    '''
    A Prestation inheriting from IntCol
    '''
    def __init__(self, func, **kwargs):
        IntCol.__init__(self, **kwargs)
        Prestation.__init__(self, func, **kwargs)

class EnumPresta(Prestation, EnumCol):
    '''
    A Prestation inheriting from EnumCol
    '''
    def __init__(self, func, enum = None, **kwargs):
        EnumCol.__init__(self, enum = enum, **kwargs)
        Prestation.__init__(self, func, **kwargs)


#    def dep_resolve(self, resolved=set(), unresolved=set()):
#        '''
#        Dependency solver.
#        Algorithm found here http://www.electricmonk.nl/log/2008/08/07/dependency-resolving-algorithm/
#        '''
#        edges = self._parents
#        unresolved.add(self)
#        for edge in edges:
#            if edge not in resolved:
#                if edge in unresolved:
#                    raise Exception('Circular reference detected: %s -> %s' % (self._name, edge._name))
#                edge.dep_resolve(resolved, unresolved)
#        
#        resolved.add(self)
#        unresolved.remove(self)
