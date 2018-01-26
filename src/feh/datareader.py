import datetime as dt
import functools
import pandas as pd
import os
import logging
import shutil
import sqlalchemy
from configobj import ConfigObj
from pandas import DataFrame
from feh.utils import dec_err_handler, get_files


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

    @classmethod
    def print_cfg(cls):
        for key, value in cls.config.iteritems():
            print(key, value, '\n')

    def log_dataload(self, source, dest, file):
        """ For logging data loads. Timestamp used is current time.
        :param source: data source system (eg: 'opera').
        :param dest: data dest system (eg: 'mysql').
        :param file: Filename. To log each filename (don't include the path).
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

    # def read(self, str_filename):
    #     ''' Reads the filename/data source and saves the data frame as a class attribute.
    #     Each subclass is responsible for handling the idiosyncrasies of its respective data source.
    #     :param str_filename:
    #     :return: DataFrame
    #     '''
    #     raise NotImplementedError

    # def read_all(self, str_dir):
    #     """ Given a directory name, read all the files (of a certain type) in that directory.
    #
    #     Results are saved as a class attribute.
    #     :return: DataFrame
    #     """
    #     raise NotImplementedError

    # def to_excel(self, filename):
    #     """ Writes the df_data DataFrame contents to Excel.
    #
    #     Reference: https://stackoverflow.com/questions/29463274/simulate-autofit-column-in-xslxwriter; http://xlsxwriter.readthedocs.io/example_pandas_column_formats.html,
    #     """
    #     # Create a Pandas Excel writer using XlsxWriter as the engine.
    #     writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    #     self.df_data.to_excel(writer, index=False)  # Creates the Worksheet='Sheet1'.
    #
    #     # Set column widths to be same length as column names (so that will display column names) #
    #     # Get the xlsxwriter worksheet object.
    #     worksheet = writer.sheets['Sheet1']
    #
    #     l_col_lengths = [len(x) for x in self.df_data.columns]
    #     for i, width in enumerate(l_col_lengths):
    #         worksheet.set_column(i, i, width)
    #     writer.save()
    #     writer.close()


class OperaDataReader(DataReader):
    """ For reading Opera files, which are output by Vision (on SQ's laptop), then copied to SFTP server.
    Files:
        - (DONE) Reservation Analytics 11Jul15 fwd 60 days.xlsx
        - (DONE) Reservation Analytics 11Jul15 fwd 61 days.xlsx
        - (DONE) Reservation Analytics RES -History.xlsx
        # TODO Remaining 2 Opera files. Lower priority since not really used.
        - (ON HOLD. NOT REALLY USED) CAG.xlsx
        - (ON HOLD. NOT REALLY USED) Reservation Analytics 20 Jul15 Fwd Cancellations.xlsx
    """
    df_data = DataFrame()  # Instance attribute which holds the data read in, if any.
    # self.logger -> instance variable.

    def __init__(self):
        super().__init__()

        # LOGGING #
        self.logger = logging.getLogger('opera_datareader')
        if self.logger.hasHandlers():  # Clear existing handlers, else will have duplicate logging messages.
            self.logger.handlers.clear()
        # Create the handler for the main logger
        str_fn_logger = os.path.join(self.config['global']['global_root_folder'], self.config['data_sources']['opera']['logfile'])
        fh_logger = logging.FileHandler(str_fn_logger)
        str_format = '[%(asctime)s] - [%(levelname)s] - %(message)s'
        fh_logger.setFormatter(logging.Formatter(str_format))
        self.logger.addHandler(fh_logger)  # Add the handler to the base logger
        self.logger.setLevel(logging.INFO)  # By default, logging will start at 'WARNING' unless we tell it otherwise.

    def __del__(self):
        super().__del__()

        # Logging. Close all file handlers to release the lock on the open files.
        handlers = self.logger.handlers[:]  # https://stackoverflow.com/questions/15435652/python-does-not-release-filehandles-to-logfile
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)

    # TODO Remove this test code.
    @dec_err_handler(retries=3)
    def test_decorator(self, x, y):
        print(f'{x} divided by {y} is {x / y}')

    def load_otb(self, pattern=None):
        """ Loads Opera OTB file which matches the pattern. There should only be 1 per day.
        :param pattern: Regular expression, to use for filtering file name.
        :return:
        """
        SOURCE = 'opera'
        DEST = 'mysql'
        is_history_file = 'History' in pattern

        str_folder = self.config['data_sources']['opera']['root_folder']
        str_fn_with_path, str_fn = get_files(str_folder=str_folder, pattern=pattern, latest_only=True)

        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=str_fn):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {str_fn}'
            self.logger.error(str_err)
            raise Exception(str_err)

        # Copy file to server first
        str_fn_dst = os.path.join(self.config['global']['global_temp'], str_fn)
        shutil.copy(str_fn_with_path, str_fn_dst)
        self.logger.info(f'Reading from {str_fn_with_path}, using local copy {str_fn_dst}')

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
        self.logger.info('Writing to database.')
        if is_history_file:  # The History (aka: Actuals) file is processed slightly differently.
            df_merge_nonrev.drop(labels=['snapshot_dt'], axis=1, inplace=True)
            df_merge_rev.drop(labels=['snapshot_dt'], axis=1, inplace=True)
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
        self.load_otb(pattern='History.xlsx$')  # History / Actuals

