# -*- coding:utf-8 -*-
#
# This file is part of OpenFisca.
# OpenFisca is a socio-fiscal microsimulation software
# Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul
# Licensed under the terms of the GPL (version 3 or later) license
# (see openfisca/__init__.py for details)

'''
Created on 24 juil. 2013

@author: Jérôme SANTOUL
'''
from __future__ import division
from numpy import (round, floor, zeros, maximum as max_, minimum as min_,
                   logical_not as not_)
from src.countries.france.model.data import QUIFAM

CHEF = QUIFAM['chef']
PART = QUIFAM['part']
ENFS = [QUIFAM['enf1'], QUIFAM['enf2'], QUIFAM['enf3'], QUIFAM['enf4'], QUIFAM['enf5'], QUIFAM['enf6'], QUIFAM['enf7'], QUIFAM['enf8'], QUIFAM['enf9'], ]

def _af_uniform_on_children(af, af_nbenf, age, smic55, ag1, ag2,
                            option={'af': CHEF, 'af_nbenf': CHEF, 'age': ENFS, 'smic55': ENFS}):
    '''
    Allocations familiales - réparti sur les enfants
    'ind'
    '''
    af_per_child = af/af_nbenf
    elig = smic55 * ((ag1 <= age) & (age <= ag2)) #Mettre false pour les enfants trop vieux et ceux qui gagnent plus que 55% du smic
    af_enf = elig* af_per_child
    
    return af_enf # annualisé