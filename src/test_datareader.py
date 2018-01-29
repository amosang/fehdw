import pandas as pd
import os
import re
import datetime as dt
from feh.datareader import OperaDataReader, OTAIDataReader, FWKDataReader
from feh.utils import get_files


TEST_CASE = 'load_files'     ### TEST CASE!

if TEST_CASE == 'load_files':
    fwk_dr = FWKDataReader()
    fwk_dr.remove_log_dataload('fwk', 'mysql', str_date='2018-01-29')
    fwk_dr.load()


if TEST_CASE == 'has_exceeded_dataload_freq':
    odr = OperaDataReader()
    print(odr.has_exceeded_dataload_freq('opera', 'mysql', 'Reservation Analytics 11Jul15 fwd 61 days.xlsx'))


if TEST_CASE == 'log_dataload':
    odr = OperaDataReader()
    odr.log_dataload('opera', 'mysql', 'Reservation Analytics 11Jul15 fwd 61 days.xlsx')

if TEST_CASE == 'read_files':
    odr = OperaDataReader()
    str_folder = odr.config['data_sources']['opera']['root_folder']
    str_fn = get_files(str_folder=str_folder, pattern='60 days.xlsx$', latest_only=True)
    odr.logger.info(f'Loading {str_fn}')

if TEST_CASE == 'decorator':
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

