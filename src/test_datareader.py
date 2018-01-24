import pandas as pd
import os
import re
import datetime as dt
from feh.datareader import OperaDataReader
from configobj import ConfigObj
from pandas import DataFrame

test_case = 'decorator'  #### TEST CASE!


if test_case == 'read_files':
    odr = OperaDataReader()
    str_folder = odr.config['data_sources']['opera']['root_folder']
    odr.logger.info(dt.datetime.today())

    print(str_folder)


if test_case == 'decorator':
    odr = OperaDataReader()
    odr.test_decorator(4, 0)

# # str_date = dt.datetime.today().strftime('%d%b%Y').upper()
# str_date = '02JAN2018'  # DEBUG - HARDCODED!
# str_fn_61 = 'OP_RES_{}_AM_61.xlsx'.format(str_date)
# str_fn_60 = 'OP_RES_{}_AM_60.xlsx'.format(str_date)
#
#
# print(os.path.join(str_folder, str_fn_61))
# # DataReader.config['data_sources']['opera']['userid']

