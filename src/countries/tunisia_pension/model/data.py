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

from src.lib.description import ModelDescription
from src.lib.columns import IntCol, EnumCol, BoolCol, AgesCol
from src.lib.utils import Enum


QUIFOY = Enum(['vous', 'conj', 'enf1','enf2','enf3','enf4','enf5','enf6','enf7','enf8','enf9'])
QUIMEN = Enum(['pref', 'cref', 'enf1','enf2','enf3','enf4','enf5','enf6','enf7','enf8','enf9'])
REG    = Enum(['rsna', 'rsa', 'rsaa', 'rtns', 'rtte', 're', 'rtfr', 'raic', 'cnrps_sal', 'cnrps_pen'])



# raic -> raci

class InputDescription(ModelDescription):
    '''
    Socio-economic data
    Donnée d'entrée de la simulation à fournir à partir d'une enquète ou 
    à générer avec un générateur de cas type
    '''
    
    noi = IntCol()
    idmen = IntCol() 
    idfoy = IntCol() 

    quimen  = EnumCol(QUIMEN)
    quifoy  = EnumCol(QUIFOY)
    
    statmarit = BoolCol()
    loyer = IntCol()
    
    sal0 = IntCol()
    sal1 = IntCol()
    sal2 = IntCol()
    sal3 = IntCol()
    sal4 = IntCol()
    sal5 = IntCol()
    sal6 = IntCol()
    sal7 = IntCol()
    sal8 = IntCol()
    sal9 = IntCol()
    sal10 = IntCol()
    
    age = AgesCol(default=55)
    nb_trim_val = IntCol(default=30)
    regime = EnumCol(REG)
    