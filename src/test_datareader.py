import pandas as pd
import os
import re
import datetime as dt
from feh.datareader import DataReader, OperaDataReader, OTAIDataReader, FWKDataReader, EzrmsDataReader, EloquaB2CDataReader
from feh.utils import *


TEST_CASE = 'test_elq'     ### TEST CASE!

if TEST_CASE == 'test_elq':
    elq_dr = EloquaB2CDataReader()
    elq_dr.load_activity(str_act_type='EmailSend', str_dt_from='2017-01-01', str_dt_to='2018-01-01')

if TEST_CASE == 'test_logger':
    dr = DataReader()
    dr.log_dataload('src', 'dest', 'abc.txt')   # sqlalchemy.exc.ProgrammingError:


if TEST_CASE == 'test_otai':
    otai_dr = OTAIDataReader()
    otai_dr.load_rates()


if TEST_CASE == 'reset':
    # Delete all table entries, and clear out all logs!
    print('TRUNCATING TABLES AND ARCHIVING LOGS')
    #db_truncate_tables()
    archive_logs(truncate=True)


if TEST_CASE == 'opera_load_files':
    op_dr = OperaDataReader()
    #op_dr.remove_log_dataload('opera', 'mysql', str_date='2018-02-01')
    op_dr.load()

if TEST_CASE == 'ezrms_load_files':
    ezrms_dr = EzrmsDataReader()
    str_date = dt.datetime.strftime(dt.datetime.today(), format='%Y-%m-%d')  # Today
    ezrms_dr.remove_log_dataload('ezrms', 'mysql', str_date=str_date)
    ezrms_dr.load()

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

