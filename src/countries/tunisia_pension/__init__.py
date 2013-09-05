# -*- coding: utf-8 -*-

# This file is part of OpenFisca
# Copyright © 2012 Mahdi Ben Jelloul, Clément Schaff 
# Licensed under the terms of the GPL License v3 or later version
# (see src/__init__.py for details)


# Model parameters
ENTITIES_INDEX = ['men', 'foy']

# Some variables needed by the test case plugins
CURRENCY = u"DT"


# REVENUES_CATEGORIES 

XAXIS_PROPERTIES = { 'sal0': {
                              'name' : 'sal0',
                              'typ_tot' : {'salsuperbrut' : 'Salaire super brut',
                                           'salbrut': 'Salaire brut',
                                           'sal':  'Salaire imposable',
                                           'salnet': 'Salaire net'},
                              'typ_tot_default' : 'sal'},
                             }