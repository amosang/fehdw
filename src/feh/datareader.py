import datetime as dt
import time
import functools
import pandas as pd
import os
import sys
import re
import io
import logging
import shutil
import requests
import sqlalchemy
from configobj import ConfigObj
from pandas import DataFrame
from feh.utils import dec_err_handler, get_files, MaxDataLoadException


class DataReader(object):
    config = ConfigObj('C:/fehdw/src/fehdw.conf')

    def __init__(self):
        # CREATE CONNECTION TO DB #  All data movements involve the database, so this is very convenient to have.
        str_host = self.config['data_sources']['mysql']['host']
        str_userid = self.config['data_sources']['mysql']['userid']
        str_password = self.config['data_sources']['mysql']['password']
        str_db = self.config['data_sources']['mysql']['db']
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_db}?charset=utf8'
        engine = sqlalchemy.create_engine(str_conn_mysql, echo=False)
        self.db_conn = engine.connect()

    def __del__(self):
        self.db_conn.close()

    def _init_logger(self, logger_name, source_name):
        """ Initialize self.logger for DataReader sub-classes, with 2 file handlers (1 for source, 1 for global log).
        :param logger_name: Unique name for logger class. Format: "<source_name>_datareader".
        :param source_name: Unique name for each data source. Comes from CONF file ("source_name").
        :return: NA
        """
        self.logger = logging.getLogger(logger_name)
        if self.logger.hasHandlers():  # Clear existing handlers, else will have duplicate logging messages.
            self.logger.handlers.clear()
        # Create the handler for the main logger
        str_fn_logger = os.path.join(self.config['global']['global_root_folder'], self.config['data_sources']['ezrms']['logfile'])
        fh_logger = logging.FileHandler(str_fn_logger)
        str_fn_logger_global = os.path.join(self.config['global']['global_root_folder'], self.config['global']['global_log'])
        fh_logger_global = logging.FileHandler(str_fn_logger_global)

        str_format = '[%(asctime)s]-[%(levelname)s]- %(message)s'
        str_format_global = f'[%(asctime)s]-[%(levelname)s]-[{source_name}] %(message)s'
        fh_logger.setFormatter(logging.Formatter(str_format))
        fh_logger_global.setFormatter(logging.Formatter(str_format_global))
        self.logger.addHandler(fh_logger)  # Add handler.
        self.logger.addHandler(fh_logger_global)  # Add global handler.
        self.logger.setLevel(logging.INFO)  # By default, logging will start at 'WARNING' unless we tell it otherwise.

    def _free_logger(self):
        """ Frees up all file handlers. Method is to be called on __del__().
        :return:
        """
        # Logging. Close all file handlers to release the lock on the open files.
        handlers = self.logger.handlers[:]  # https://stackoverflow.com/questions/15435652/python-does-not-release-filehandles-to-logfile
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)

    @classmethod
    def print_cfg(cls):
        for key, value in cls.config.iteritems():
            print(key, value, '\n')

    def log_dataload(self, source, dest, file):
        """ For logging data loads. Timestamp used is current time.
        :param source: data source system (eg: 'opera').
        :param dest: data dest system (eg: 'mysql').
        :param file: Filename. To log each filename (don't include the path). "file" can also be the API type (eg: rates).
        :return:
        """
        str_sql = """INSERT INTO sys_log_dataload (source, dest, timestamp, file) VALUES ('{}', '{}', '{}', '{}')
        """.format(source, dest, dt.datetime.now(), file)
        pd.io.sql.execute(str_sql, self.db_conn)

    def has_exceeded_dataload_freq(self, source, dest, file):
        """ To call before loading any data source, to ensure that loading freq does not exceed maximum loads per day.
        :param source: data source system (eg: 'opera').
        :param dest: data dest system (eg: 'mysql').
        :param file: Filename. To log each filename (don't include the path).
        :return:
        """
        # Get number of data loads within the same day (Key: source/dest/file).
        str_sql = """
        SELECT * FROM sys_log_dataload
        WHERE source = '{}'
        AND dest = '{}'
        AND file = '{}'
        AND timestamp >= '{}' AND timestamp < '{}' 
        """.format(source, dest, file, dt.datetime.now().date(), dt.datetime.now().date() + dt.timedelta(days=1))
        df = pd.read_sql(str_sql, self.db_conn)
        i_log_count = len(df)  # Number of existing log entries.

        ###
        str_sql = """
        SELECT * FROM sys_cfg_dataload
        WHERE source = '{}'
        AND dest = '{}'
        AND file = '{}'
        """.format(source, dest, file)  # Check for specific file's cfg entry first.
        df = pd.read_sql(str_sql, self.db_conn)

        if len(df) == 0:
            str_sql = """
            SELECT * FROM sys_cfg_dataload
            WHERE source = '{}'
            AND dest = '{}'
            AND file = '*'
            """.format(source, dest)  # Check for the '*' entry.
            df = pd.read_sql(str_sql, self.db_conn)
            i_cfg_count = int(df['max_freq'][0])
        else:
            i_cfg_count = int(df['max_freq'][0])  # A file specific cfg entry has been found!

        if i_log_count >= i_cfg_count:  # When it's equal, it means max_freq is hit already.
            return True  # Exceeded max_freq!
        else:
            return False

    def remove_log_dataload(self, source, dest, str_date, file=None):
        """ Removes one or more data load log entries.
        :param source:
        :param dest:
        :param str_date: Format: "YYYY-MM-DD". The date for which the log entries are to be deleted.
        :param file: Optional. If not given, will delete all log entries for the key combination of source/dest/timestamp.
        :return: None
        """
        if file is not None:
            str_sql = """
            DELETE FROM sys_log_dataload
            WHERE source = '{}'
            AND dest = '{}'
            AND DATE(timestamp) = '{}'
            AND file = '{}'
            """.format(source, dest, str_date, file)
        else:
            str_sql = """
            DELETE FROM sys_log_dataload
            WHERE source = '{}'
            AND dest = '{}'
            AND DATE(timestamp) = '{}'
            """.format(source, dest, str_date)

        pd.io.sql.execute(str_sql, self.db_conn)

class OperaDataReader(DataReader):
    """ For reading Opera files, which are output by Vision (run on SQ's laptop), then copied to SFTP server.
    Files:
        - (DONE) Reservation Analytics 11Jul15 fwd 60 days.xlsx
        - (DONE) Reservation Analytics 11Jul15 fwd 61 days.xlsx
        - (DONE) Reservation Analytics RES -History.xlsx
        # TODO Remaining 2 Opera files. Lower priority since not really used.
        - (ON HOLD. NOT REALLY USED) CAG.xlsx
        - (ON HOLD. NOT REALLY USED) Reservation Analytics 20 Jul15 Fwd Cancellations.xlsx
    """
    def __init__(self):
        super().__init__()
        self.SOURCE_NAME = self.config['data_sources']['opera']['source_name']
        self._init_logger(logger_name=self.SOURCE_NAME + '_datareader', source_name=self.SOURCE_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    @dec_err_handler()  # Logs unforeseen exceptions. Put here, so that the next file load will be unaffected if earlier one fails.
    def load_otb(self, pattern=None):
        """ Loads Opera OTB file which matches the pattern. There should only be 1 per day.
        Also used to load History/Actuals file, because it has a similar format.
        :param pattern: Regular expression, to use for filtering file name.
        :return: None
        """
        SOURCE = self.SOURCE_NAME  # 'opera'
        DEST = 'mysql'
        is_history_file = 'History' in pattern

        str_folder = self.config['data_sources']['opera']['root_folder']
        str_fn_with_path, str_fn = get_files(str_folder=str_folder, pattern=pattern, latest_only=True)

        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=str_fn):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {str_fn}'
            raise MaxDataLoadException(str_err)

        # Copy file to server first. Rename files to add timestamp, because the incoming files ALWAYS have the same names, so will be overwritten.
        str_fn = str_fn[:-5] + '-' + dt.datetime.strftime(dt.datetime.today(), '%Y%m%d_%H%M') + str_fn[-5:]
        str_fn_dst = os.path.join(self.config['global']['global_temp'], str_fn)
        shutil.copy(str_fn_with_path, str_fn_dst)
        self.logger.info(f'Reading file "{str_fn_with_path}". Local copy "{str_fn_dst}"')

        # READ THE FILES #
        df_reservation = pd.read_excel(str_fn_dst, sheet_name='Reservation', keep_default_na=False, na_values=[' '])
        df_allotment = pd.read_excel(str_fn_dst, sheet_name='Allotment', keep_default_na=False, na_values=[' '])
        df_arr_depart = pd.read_excel(str_fn_dst, sheet_name='Arr Dept', keep_default_na=False, na_values=[' '])

        if '60' in pattern:  # Legacy. This specific tab is somehow named differently in each file!
            df_sales = pd.read_excel(str_fn_dst, sheet_name='Sales (for Corporate Production', keep_default_na=False, na_values=[' '])
        else:  # The "61" file here. For the "History" file as well.
            df_sales = pd.read_excel(str_fn_dst, sheet_name='Sales', keep_default_na=False, na_values=[' '])

        df_guest_profile = pd.read_excel(str_fn_dst, sheet_name='Guest profile', keep_default_na=False, na_values=[' '])
        df_no_of_guest = pd.read_excel(str_fn_dst, sheet_name='No of guest', keep_default_na=False, na_values=[' '])
        df_rev_vhac_vhb = pd.read_excel(str_fn_dst, sheet_name='Revenue VHAC VHB', keep_default_na=False, na_values=[' '])
        df_rev_vhc_vhk = pd.read_excel(str_fn_dst, sheet_name='Revenue VHC VHK', keep_default_na=False, na_values=[' '])
        df_rev_ohs_tqh = pd.read_excel(str_fn_dst, sheet_name='Revenue OHS TQH', keep_default_na=False, na_values=[' '])
        df_rev_oph_tes = pd.read_excel(str_fn_dst, sheet_name='Revenue OPH TES', keep_default_na=False, na_values=[' '])
        df_rev_rhs_amoy = pd.read_excel(str_fn_dst, sheet_name='Revenue RHS AMOY', keep_default_na=False, na_values=[' '])

        # Renaming columns manually. Assumes that source Excel files do not change column positions, or added/deleted columns!
        if is_history_file:
            df_reservation.columns = ['confirmation_no', 'resort', 'res_reserv_status', 'res_bkdroom_ct_lbl',
                                      'res_roomty_lbl', 'res_reserv_date', 'res_cancel_date', 'res_pay_method',
                                      'res_credcard_ty', 'res_business_date']
        else:
            df_reservation.columns = ['confirmation_no', 'resort', 'res_reserv_status', 'res_bkdroom_ct_lbl',
                                      'res_roomty_lbl', 'res_reserv_date', 'res_cancel_date', 'res_pay_method',
                                      'res_credcard_ty']

        df_allotment.columns = ['confirmation_no', 'resort', 'allot_allot_book_cde', 'allot_allot_book_desc']

        df_arr_depart.columns = ['confirmation_no', 'resort', 'arr_arrival_dt', 'arr_departure_dt', 'arr_arrival_time',
                                 'arr_departure_time']

        df_sales.columns = ['confirmation_no', 'resort', 'sale_comp_name', 'sale_zip_code', 'sale_sales_rep',
                            'sale_sales_rep_code', 'sale_industry_code', 'sale_trav_agent_name', 'sale_sales_rep1',
                            'sale_sales_rep_code1', 'sale_all_owner']

        df_guest_profile.columns = ['confirmation_no', 'resort', 'gp_guest_ctry_cde', 'gp_nationality',
                                    'gp_origin_code', 'gp_guest_lastname', 'gp_guest_firstname',
                                    'gp_guest_title_code', 'gp_spcl_req_desc', 'gp_email', 'gp_dob',
                                    'gp_vip_status_desc']

        df_no_of_guest.columns = ['confirmation_no', 'resort', 'ng_adults', 'ng_children', 'ng_extra_beds', 'ng_cribs']

        l_rev_cols = ['confirmation_no', 'resort', 'business_dt', 'rev_marketcode', 'rev_ratecode', 'rev_sourcecode',
                      'rev_sourcename', 'rev_eff_rate_amt', 'rev_actual_room_nts', 'rev_rmrev_extax',
                      'rev_food_rev_inctax', 'rev_oth_rev_inctax', 'rev_hurdle', 'rev_hurdle_override',
                      'rev_restriction_override']

        df_rev_ohs_tqh.columns = l_rev_cols
        df_rev_oph_tes.columns = l_rev_cols
        df_rev_rhs_amoy.columns = l_rev_cols
        df_rev_vhac_vhb.columns = l_rev_cols
        df_rev_vhc_vhk.columns = l_rev_cols

        # Merge all the Revenue dfs. They were originally separated due to bandwidth limitations of the "Vision" extraction tool.
        df_merge_rev = pd.concat([df_rev_ohs_tqh, df_rev_oph_tes, df_rev_rhs_amoy, df_rev_vhac_vhb, df_rev_vhc_vhk])
        df_merge_rev['snapshot_dt'] = dt.datetime.today()

        # Merge all the non-Revenue dfs, using Reservation as the base. They are said to be of 1-to-1 relationship.
        l_dfs = [df_reservation, df_allotment, df_arr_depart, df_sales, df_guest_profile, df_no_of_guest]
        df_merge_nonrev = functools.reduce(lambda left, right: pd.merge(left, right, on=['confirmation_no', 'resort']), l_dfs)
        df_merge_nonrev['snapshot_dt'] = dt.datetime.today()

        # WRITE TO DATABASE #
        self.logger.info('Loading file contents to data warehouse.')
        if is_history_file:  # The History (aka: Actuals) file is processed slightly differently.
            # Leave "snapshot_dt". It serves as a handle to extract wrongly loaded data, if any.
            # df_merge_nonrev.drop(labels=['snapshot_dt'], axis=1, inplace=True)
            # df_merge_rev.drop(labels=['snapshot_dt'], axis=1, inplace=True)
            df_merge_nonrev.to_sql('stg_op_act_nonrev', self.db_conn, index=False, if_exists='append')
            df_merge_rev.to_sql('stg_op_act_rev', self.db_conn, index=False, if_exists='append')
        else:  # The other 2 files "60" and "61".
            df_merge_nonrev.to_sql('stg_op_otb_nonrev', self.db_conn, index=False, if_exists='append')
            df_merge_rev.to_sql('stg_op_otb_rev', self.db_conn, index=False, if_exists='append')

        # LOG DATALOAD #
        self.logger.info('Logged data load activity to system log table.')
        self.log_dataload('opera', 'mysql', str_fn)

        # TODO Uncomment this line when system has stabilized, to delete temp files.
        # Remove the temp file copy.
        #os.remove(str_fn_dst)

    def load(self):
        """ Loads data from a related cluster of data sources.
        :return:
        """
        self.load_otb(pattern='60 days.xlsx$')  # OTB 0-60 days onwards. Currently always picks the last modified file with this file name.
        self.load_otb(pattern='61 days.xlsx$')  # OTB 61 days onwards.
        self.load_otb(pattern='History.xlsx$')  # History (aka: Actuals)

class OTAIDataReader(DataReader):
    """ Class for interfacing with the OTA Insight API data source.
    """
    def __init__(self):
        super().__init__()
        self.SOURCE_NAME = self.config['data_sources']['otai']['source_name']
        self._init_logger(logger_name=self.SOURCE_NAME + '_datareader', source_name=self.SOURCE_NAME)

        # SET GLOBAL PARAMETERS #
        self.API_TOKEN = self.config['data_sources']['otai']['token']
        self.BASE_URL = self.config['data_sources']['otai']['base_url']

        # BUFFER HotelIDs and CompsetID INTO MEMORY #
        str_sql = """SELECT hotel_id, hotel_name, comp_id, comp_name FROM stg_otai_hotels WHERE hotel_category = 'hotel'"""
        df = pd.read_sql(str_sql, self.db_conn)  # Read HotelIDs/CompsetIDs into buffer for future querying.
        if len(df) != 0:
            self.df_hotels = df
        else:  # Make API call only if there's no data. More resilient.
            self.load_hotels()
            df = pd.read_sql(str_sql, self.db_conn)
            self.df_hotels = df  # Class attribute.

    def __del__(self):
        super().__del__()
        self._free_logger()

    def load_hotels(self):
        """ Loads Hotels and Compset IDs to data warehouse. We need the IDs for further queries (eg: rates).
        We should not have to call this frequently. Perhaps can periodically refresh, to avoid API calls.
        """
        res = requests.get(self.BASE_URL + 'hotels', params={'token': self.API_TOKEN, 'format': 'csv'})  # Better to use requests module. Can check for "requests.ok" -> HTTP 200.
        df_hotels = pd.read_csv(io.StringIO(res.content.decode('utf-8')))
        df_hotels.columns = ['hotel_id', 'hotel_name', 'hotel_stars', 'comp_id', 'comp_name', 'comp_stars', 'compset_id', 'compset_name']

        # New column. Label the dataset with known Hotels; remainder are deemed to be SRs.
        l_hotelid = [25002, 25105, 25106, 25107, 25109, 25119, 25167, 296976, 359549, 613872, 1663218]  # 11 hotels
        df_hotels['hotel_category'] = 'sr'  # default value.
        df_hotels['hotel_category'][df_hotels['hotel_id'].isin(l_hotelid)] = 'hotel'

        df_hotels.to_sql('stg_otai_hotels', self.db_conn, index=False, if_exists='replace')

    def get_rates_hotel(self, str_hotel_id=None, format='csv', ota='bookingdotcom'):
        # Defaults: los: 1, persons:2, mealType: nofilter (lowest), shopLength: 90 (max: 365), changeDays: None (max: 3 values, max: 30 days), compsetIds=1
        # Not using changeDays (we will calculate it ourselves).
        params = {'token': self.API_TOKEN,
                  'format': format,
                  'hotelId': str_hotel_id,  # parameter names are case sensitive.
                  'ota': ota
                  }
        res = requests.get(self.BASE_URL + 'rates', params=params)

        if res.status_code == 200:  # HTTP 200
            df = pd.read_csv(io.StringIO(res.content.decode('utf-8')))
            df['ArrivalDate'] = pd.to_datetime(df['ArrivalDate'])
            df['ExtractDateTime'] = pd.to_datetime(df['ExtractDateTime'])
            return df
        elif res.status_code == 429:  # 429 Too Many Requests. API calls are rate-limited to 120 requests/min per token.
            str_err = 'Hotel Rates: HTTP 429 Too Many Requests'
            raise Exception(str_err)

    def load_rates(self):
        SOURCE = self.SOURCE_NAME  #'otai'
        DEST = 'mysql'
        FILE = 'rates'
        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=FILE):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {FILE}'
            raise MaxDataLoadException(str_err)

        df_all = DataFrame()  # accumulator

        self.logger.info('Hotel Rates: Reading from API')
        for hotel_id in self.df_hotels['hotel_id']:
            df = self.get_rates_hotel(str_hotel_id=str(hotel_id))
            df_all = df_all.append(df, ignore_index=True)

        self.logger.info('Hotel Rates: Loading to data warehouse')
        df_all['snapshot_dt'] = dt.datetime.today()  # Add a timestamp when the data was loaded.
        df_all.to_sql('stg_otai_rates', self.db_conn, index=False, if_exists='append')

        # LOG DATALOAD #
        self.logger.info('Hotel Rates: Logged data load activity to system log table.')
        self.log_dataload(source='otai', dest='mysql', file='rates')

    def load(self):
        """ Loads data from a related cluster of data sources.
        """
        self.load_rates()


class FWKDataReader(DataReader):
    """ For reading FWK files from SFTP server.
    Files:
    - (DONE) FWK_PROJ_29JAN2018.csv
    - (DONE) FWK_29JAN2018_SCREEN1.csv
    """
    def __init__(self):
        super().__init__()
        self.SOURCE_NAME = self.config['data_sources']['fwk']['source_name']
        self._init_logger(logger_name=self.SOURCE_NAME + '_datareader', source_name=self.SOURCE_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    @dec_err_handler()  # Logs unforeseen exceptions. Put here, so that the next file load will be unaffected if earlier one fails.
    def load_projected_and_otb(self, pattern=None, type=None):
        """ Loads FWK Projected filename which matches the given pattern. There should only be 1 per day.
        Also used to load FWK OTB file, because it has a similar format.
        :param pattern: Regular expression, to use for filtering file name.
        :return: None
        """
        SOURCE = self.SOURCE_NAME  #'fwk'
        DEST = 'mysql'

        str_folder = self.config['data_sources']['fwk']['root_folder']
        str_fn_with_path, str_fn = get_files(str_folder=str_folder, pattern=pattern, latest_only=True)

        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=str_fn):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {str_fn}'
            raise MaxDataLoadException(str_err)

        # Copy file to server first. Incoming file names already have timestamp in them.
        str_fn_dst = os.path.join(self.config['global']['global_temp'], str_fn)
        shutil.copy(str_fn_with_path, str_fn_dst)
        self.logger.info(f'Reading file "{str_fn_with_path}". Local copy "{str_fn_dst}"')

        # READ THE FILES #
        df = pd.read_csv(str_fn_dst)
        df = df.iloc[:, :-1]  # Drop the last column. Blank.

        # Typecasting
        df['year'] = df['year'].astype(str)
        df['periodFrom'] = pd.to_datetime(df['periodFrom'], format='%d/%m/%Y')
        df['periodTo'] = pd.to_datetime(df['periodTo'], format='%d/%m/%Y')
        df['snapshot_dt'] = dt.datetime.today()  # Add timestamp as handle.

        self.logger.info('Loading file contents to data warehouse.')
        if type == 'projected':
            df.to_sql('stg_fwk_proj', self.db_conn, index=False, if_exists='append')
        elif type == 'otb':
            df.to_sql('stg_fwk_otb', self.db_conn, index=False, if_exists='append')

        # LOG DATALOAD #
        self.logger.info('Logged data load activity to system log table.')
        self.log_dataload('fwk', 'mysql', str_fn)

        # TODO Uncomment this line when system has stabilized, to delete temp files.
        #Remove the temp file copy.
        #os.remove(str_fn_dst)

    def load(self):
        """ Loads data from a related cluster of data sources.
        """
        self.load_projected_and_otb(pattern='^FWK_PROJ.+csv$', type='projected')  # eg: FWK_PROJ_29JAN2018.csv
        self.load_projected_and_otb(pattern='^FWK.+SCREEN1.csv$', type='otb')  # eg: FWK_29JAN2018_SCREEN1.csv

class EzrmsDataReader(DataReader):
    """ For scrapping EzRMS Forecast report from EzRMS website.
    """
    def __init__(self):
        super().__init__()
        self.SOURCE_NAME = self.config['data_sources']['ezrms']['source_name']
        self._init_logger(logger_name=self.SOURCE_NAME + '_datareader', source_name=self.SOURCE_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    @dec_err_handler(retries=3)
    def load_forecast(self):
        """ Download and load the Forecast TSV file. Transforms the data set from wide to long as well.
        Note: If the layout of the Forecast TSV file changes (eg: due to addition of new hotels), the code to read the
        source file needs to change too. The database table schema however, is already in long form and can continue as-is.

        Important: hotel_code = 'ALL' is persisted into the table as well! Remember to exclude when doing aggregations.
        """
        from selenium import webdriver
        from selenium.common.exceptions import NoSuchElementException

        SOURCE = self.SOURCE_NAME  # 'ezrms'
        DEST = 'mysql'
        FILE = 'forecast'

        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=FILE):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {FILE}'
            raise MaxDataLoadException(str_err)

        self.logger.info('Initiating Chrome webdriver and logging in to EzRMS')
        str_fn_chromedriver = os.path.join(self.config['global']['global_bin'], 'chromedriver_2.34.exe')
        driver = webdriver.Chrome(executable_path=str_fn_chromedriver)  # NOTE: Check for presence of 'options!'.
        driver.get('https://bw8.ezrms.infor.com/ezmingle.isp')
        driver.switch_to.frame('Infor Generic Application')
        driver.switch_to.frame('main')

        input_email = driver.find_element_by_xpath('//*[@id="email"]')
        input_email.send_keys(self.config['data_sources']['ezrms']['userid'])
        input_password = driver.find_element_by_xpath('//*[@id="password"]')
        input_password.send_keys(self.config['data_sources']['ezrms']['password'])
        input_password.submit()  # Walks up the tree until it finds the enclosing Form, and submits that. http://selenium-python.readthedocs.io/navigating.html

        # DOWNLOAD TSV, READ THE DATA #
        self.logger.info('Downloading Forecast TSV file')
        driver.get('https://bw8.ezrms.infor.com/fav.isp?favcat=User&favgroup=2&favid=76459&favname=Regional+Forecast+Analysis-+Big+data&favsubcat=amosang%40fareast.com.sg&period=16')
        driver.switch_to.frame('main')
        t = driver.find_elements_by_class_name('ezhiddenblock')[3]  # Thru trial-and-error. It's the third block

        # A N-digit string, eg: "117475". Done this way because this string changes with each session!
        match = re.search('\d+(?=\w+)', t.get_attribute('id'))  # Searching for some digits followed by some word characters.
        if match:  # ie: There's a match
            str_id = match.group(0)
            str_xpath = '//*[@id="group_ID{}"]/tbody/tr/td/div/table/tbody/tr/td[2]/table/tbody/tr[1]/td/span/img'.format(str_id)
        else:
            raise Exception('Unable to determine str_xpath for the "3-dots" button')  # Handled by decorator.

        try:
            driver.find_element_by_xpath(str_xpath).click()  # 3 vertical dots icon
        except NoSuchElementException:
            raise Exception('NoSuchElementException encountered when trying to download Forecast TSV file')

        driver.switch_to.frame('ezpopupselectwindow')
        driver.find_element_by_xpath('/html/body/table/tbody/tr[3]/td').click()  # Export to TSV. Apparently clicking on the img works, even though there's no on-click event on it.
        time.sleep(8)  # The TSV file downloads asynchronously in a separate process. We need the file to be present before continuing!
        str_dl_folder = os.path.join(os.getenv('USERPROFILE'), 'Downloads')  # eg: 'C:/Users/feh_admin/Downloads'
        str_fn_with_path, str_fn = get_files(str_folder=str_dl_folder, pattern='table.tsv$', latest_only=True)

        self.logger.info('Using as Forecast file: {}'.format(str_fn))
        df_forecast = pd.read_table(str_fn_with_path, header=None, skiprows=2, skipfooter=7, engine='python')

        # READING AND TRANSFORMING THE TSV FILE #
        # Typecasting and other clean ups.
        df_forecast[0] = pd.to_datetime(df_forecast[0])
        df_forecast.iloc[:, 14:] = df_forecast.iloc[:, 14:].apply(lambda x: x.str.rstrip('%').astype(
            'float') / 100)  # Convert percentage string to float. Processing 1 Series at a time, ie: column-by-column.

        df_forecast.columns = ['DATE', 'DOW', 'OCC_ACH', 'OCC_CVH', 'OCC_GLH', 'OCC_OHS', 'OCC_OPH', 'OCC_TES',
                               'OCC_TQH', 'OCC_EVH', 'OCC_RHS',
                               'OCC_OHD', 'OCC_OKL', 'OCC_ALL', 'P_ACH', 'P_CVH', 'P_GLH', 'P_OHS', 'P_OPH', 'P_TES',
                               'P_TQH', 'P_EVH',
                               'P_RHS', 'P_OHD', 'P_OKL', 'P_ALL']

        df_forecast['DATE'] = pd.to_datetime(df_forecast['DATE'])
        df_forecast.drop(labels='DOW', axis=1, inplace=True)

        df = pd.wide_to_long(df_forecast, ['OCC_', 'P_'], j='hotel_code', i='DATE', suffix='.+').reset_index()
        df.columns = ['date', 'hotel_code', 'occ_rooms', 'occ_percent']
        df['snapshot_dt'] = dt.datetime.today()
        df_forecast = df

        self.logger.info('Writing to database.')
        df_forecast.to_sql('stg_ezrms_forecast', self.db_conn, index=False, if_exists='append')

        # LOG DATALOAD #
        self.logger.info('Logged data load activity to system log table.')
        self.log_dataload('ezrms', 'mysql', FILE)

        driver.close()  # Close the browser
        os.remove(str_fn_with_path)  # Delete the TSV file.

    def load(self):
        """ Loads data from a related cluster of data sources.
        :return:
        """
        self.load_forecast()

