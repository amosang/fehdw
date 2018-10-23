import pandas as pd
import os
import re
import datetime as dt
from feh.datareader import DataReader, OperaDataReader, OTAIDataReader, FWKDataReader, EzrmsDataReader, EloquaB2CDataReader
from feh.datarunner import DataRunner
from feh.utils import *
import feh.utils

TEST_CASE = 'fix_op_2018-10-19'     ### TEST CASE!



if TEST_CASE == 'fix_op_2018-10-19':
    # op_dr = OperaDataReader()  # Cannot use DataReader() class. AttributeError: 'DataReader' object has no attribute 'logger'
    # d_run = DataRunner()
    # fwk_dr = FWKDataReader()
    # d_run.drop_and_reload_data_marts(dt_date=dt.datetime.strptime('2018-10-19', format('%Y-%m-%d')))
    # fwk_dr.remove_log_dataload('fwk', 'mysql', str_date='2018-10-19')
    # fwk_dr.load()
    #op_dr.drop_and_reload_staging_tables(dt_date=dt.datetime.strptime('2018-10-19', format('%Y-%m-%d')))
    #d_run.drop_and_reload_data_marts()
    feh.utils.get_latest_file(str_folder='//10.0.2.251/fesftp/FWK', pattern='^FWK_PROJ.+csv$')


if TEST_CASE == 'send_email_data_imputation':
    feh.utils._send_email_data_imputation(str_listname='test_aa', str_subject='TEST', l_tab_name=['stg_123', 'stg_abc'])


if TEST_CASE == 'debug_otai':
    otai_dr = OTAIDataReader()
    otai_dr.load_rates()


if TEST_CASE == 'reload_op_files':
    op_dr = OperaDataReader()
    op_dr.load()


if TEST_CASE == 'check_dataload_not_logged':
    dr = DataReader()
    check_dataload_not_logged(dt.time(0, 0), dr.db_conn)


if TEST_CASE == 'test_load_rates':
    otai_dr = OTAIDataReader()
    #otai_dr.remove_log_dataload('opera', 'mysql', str_date='2018-02-01')
    #otai_dr.get_rates_hotel(str_hotel_id=25042)
    otai_dr.load_hotels()


if TEST_CASE == 'test_op_cag':
    op_dr = OperaDataReader()
    op_dr.remove_log_dataload('opera', 'mysql', str_date='2018-02-01')
    op_dr.load_cag(pattern='CAG.xlsx$')

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

