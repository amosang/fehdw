import pandas as pd
import os
import re
import datetime as dt
from feh.datareader import OperaDataReader
from feh.utils import get_files


test_case = 'load_files'  #### TEST CASE!

if test_case == 'load_files':
    odr = OperaDataReader()
    odr.load()


if test_case == 'read_files':
    odr = OperaDataReader()
    str_folder = odr.config['data_sources']['opera']['root_folder']
    str_fn = get_files(str_folder=str_folder, pattern='60 days.xlsx$', latest_only=True)
    odr.logger.info(f'Loading {str_fn}')

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

