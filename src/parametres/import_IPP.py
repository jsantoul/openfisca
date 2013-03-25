# -*- coding:utf-8 -*-

'''
Created on 25 Mar 2013

@author: alexis eidelman
'''

import pandas
import pdb
import datetime

xls = pandas.ExcelFile('cotis2.xlsx')
print xls

test = xls.parse('PSS', index_col=None, na_values=['NA'])
pdb.set_trace()
print xls

# datetime.datetime( test['Date'])




## check type of data
# datetime in date and not string