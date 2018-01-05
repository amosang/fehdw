import pandas as pd
import os
import re
from configobj import ConfigObj
from pandas import DataFrame

from feh.datareader import DataReader, OperaDataReader

test_case = 'logging'

if test_case == 'logging':
    odr = OperaDataReader()
    odr.logger.info('TEST Message 3')


# str_date = datetime.datetime.today().strftime('%d%b%Y').upper()
str_date = '02JAN2018'  # DEBUG - HARDCODED!
str_fn_61 = 'OP_RES_{}_AM_61.xlsx'.format(str_date)
str_fn_60 = 'OP_RES_{}_AM_60.xlsx'.format(str_date)

str_folder = odr.config['data_sources']['opera']['root_folder']
print(os.path.join(str_folder, str_fn_61))
# DataReader.config['data_sources']['opera']['userid']

