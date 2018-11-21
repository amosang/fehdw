import datetime as dt
import time
import functools
import pandas as pd
import os
import re
import io
import logging
import shutil
import requests
import sqlalchemy
import pysftp
import feh.utils
from configobj import ConfigObj
from pandas import DataFrame, Series
from feh.utils import dec_err_handler, get_files, MaxDataLoadException
from pyeloqua import Bulk


class DataReader(object):
    config = ConfigObj('C:/fehdw/src/fehdw.conf')

    def __init__(self):
        # CREATE CONNECTION TO DB #  All data movements involve the database, so this is very convenient to have.
        str_host = self.config['data_sources']['mysql']['host']
        str_userid = self.config['data_sources']['mysql']['userid']
        str_password = self.config['data_sources']['mysql']['password']
        str_db = self.config['data_sources']['mysql']['db']
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_db}?charset=utf8mb4'
        engine = sqlalchemy.create_engine(str_conn_mysql, echo=False)
        self.db_conn = engine.connect()

    def __del__(self):
        self.db_conn.close()

    def _init_logger(self, logger_name, source_name=None):
        """ Initialize self.logger for DataReader sub-classes
        If source_name = None, means that we only want to log to global.log.
            Usage: "dr._init_logger(logger_name='global')"
        If source_name is an actual data source name (in fehdw.conf), then 2 file handlers will be created.

        :param logger_name: Unique name used solely in Logger class as id. Format to send in: "<source_name>_datareader".
        :param source_name: Unique name for each data source. Comes from CONF file ("source_name"). eg: "opera", "ezrms".
        :return: NA
        """
        self.logger = logging.getLogger(logger_name)  # A specific id for the Logger class use only.
        self.logger.setLevel(logging.INFO)  # By default, logging will start at 'WARNING' unless we tell it otherwise.

        if self.logger.hasHandlers():  # Clear existing handlers, else will have duplicate logging messages.
            self.logger.handlers.clear()

        # LOG TO GLOBAL LOG FILE #
        str_fn_logger_global = os.path.join(self.config['global']['global_root_folder'], self.config['global']['global_log'])
        fh_logger_global = logging.FileHandler(str_fn_logger_global)

        if source_name:  # if not None, means it's True, and that there is a data source.
            str_format_global = f'[%(asctime)s]-[%(levelname)s]-[{source_name}] %(message)s'
        else:  # Is None. Means global log only.
            str_format_global = f'[%(asctime)s]-[%(levelname)s] %(message)s'

        fh_logger_global.setFormatter(logging.Formatter(str_format_global))
        self.logger.addHandler(fh_logger_global)  # Add global handler.

        if source_name:  # Log to data source specific file, only if a source_name is given.
            # LOG TO <source_name> LOG FILE #
            str_fn_logger = os.path.join(self.config['global']['global_root_folder'],
                                         self.config['data_sources'][source_name]['logfile'])
            fh_logger = logging.FileHandler(str_fn_logger)
            str_format = '[%(asctime)s]-[%(levelname)s]- %(message)s'
            fh_logger.setFormatter(logging.Formatter(str_format))
            self.logger.addHandler(fh_logger)  # Add handler.

    def _free_logger(self):
        """ Frees up all file handlers. Method is to be called on __del__().
        :return:
        """
        # Logging. Close all file handlers to release the lock on the open files.
        handlers = self.logger.handlers[:]  # https://stackoverflow.com/questions/15435652/python-does-not-release-filehandles-to-logfile
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)

    def drop_and_reload_staging_tables(self, dt_date):
        """ For a given snapshot_dt, delete all records from ALL staging tables (ie: "stg*").
        For use in fixing the situation where 1 or more data sources failed or was corrupted, on a particular date.

        Assumptions:
        1) Whatever errors in the data sources have all been rectified.
        2) All the correct and necessary data files are in place in their corresponding folders.

        :param dt_date:
        :return: NA
        """
        str_date_from, str_date_to = feh.utils.split_date(dt_date=dt_date)

        # CREATING 1 INSTANCE OF EACH DATA READER CLASS #
        ezrms_dr = EzrmsDataReader()
        fwk_dr = FWKDataReader()
        op_dr = OperaDataReader()
        otai_dr = OTAIDataReader()

        # GET LIST OF TABLE NAMES, FOR WHICH TO DROP RECORDS #  The column_name constraint further restricts the selection to tables which have the snapshot_dt column.
        str_sql_tabs = """
        SELECT table_name FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME LIKE 'stg%%'
        AND COLUMN_NAME = 'snapshot_dt'
        ORDER BY table_name
        """  # Note the double "%" signs. It's for escaping Python, so that only 1 "%" is sent to MySQL.
        df_tabs = pd.read_sql(str_sql_tabs, self.db_conn)

        # DROP THE RELEVANT RECORDS FOR EACH TABLE #
        for idx, row in df_tabs.iterrows():
            str_tab_name = row['table_name']

            self.logger.info('DELETING STAGING TABLE: {}'.format(str_tab_name))

            str_sql = """
            DELETE FROM {}
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
            """.format(str_tab_name, str_date_from, str_date_to)  # Recall that in staging tables, snapshot_dt contains a timestamp, hence a selection range is required.

            pd.io.sql.execute(str_sql, self.db_conn)

        # DROP ALL DATA LOAD LOGS, FOR THE GIVEN DATE #
        self.logger.info('Deleting all data load logs for snapshot_dt: {}'.format(str_date_from))
        str_sql = """
        DELETE FROM sys_log_dataload
        WHERE DATE(timestamp) = '{}'
        """.format(str_date_from)

        pd.io.sql.execute(str_sql, self.db_conn)

        # RELOAD DATA FOR SPECIFIC DATE #
        self.logger.info('Reloading staging tables')
        ezrms_dr.load(dt_snapshot_dt=dt_date)
        fwk_dr.load(dt_snapshot_dt=dt_date)
        op_dr.load(dt_snapshot_dt=dt_date)
        otai_dr.load(dt_snapshot_dt=dt_date)

    def log_dataload(self, source, dest, file):
        """ For logging data loads. Timestamp used is current time.
        :param source: data source system (eg: 'opera').
        :param dest: data dest system (eg: 'mysql').
        :param file: Filename. To log each filename (don't include the path). "file" can also be the API type (eg: rates).
        :return:
        """
        try:
            str_sql = """INSERT INTO sys_log_dataload (source, dest, timestamp, file) VALUES ('{}', '{}', '{}', '{}')
            """.format(source, dest, dt.datetime.now(), file)
            pd.io.sql.execute(str_sql, self.db_conn)
        except Exception:  # sqlalchemy.exc.ProgrammingError
            # If Exception, that can only mean that the table is not there. Create the table, and do again.
            str_sql = """ CREATE TABLE `sys_log_dataload` (
            `source` text, `dest` text, `file` text, `timestamp` datetime DEFAULT NULL) ENGINE = MyISAM;
            """
            pd.io.sql.execute(str_sql, self.db_conn)
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
        # Get number of logged data loads already done within the same day (KEY: source/dest/file).
        str_sql = """
        SELECT * FROM sys_log_dataload
        WHERE source = '{}'
        AND dest = '{}'
        AND file = '{}'
        AND timestamp >= '{}' AND timestamp < '{}' 
        """.format(source, dest, file, dt.datetime.now().date(), dt.datetime.now().date() + dt.timedelta(days=1))
        df = pd.read_sql(str_sql, self.db_conn)
        i_log_count = len(df)  # Number of existing log entries.

        # Check the policy table for how many data loads should be allowed.
        str_sql = """
        SELECT * FROM sys_cfg_dataload_freq
        WHERE source = '{}'
        AND dest = '{}'
        AND file = '{}'
        """.format(source, dest, file)  # Check for specific file's cfg entry first.
        df = pd.read_sql(str_sql, self.db_conn)

        if len(df) == 0:
            str_sql = """
            SELECT * FROM sys_cfg_dataload_freq
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

    def copy_sftp_file_latest(self, str_folder_remote=None, str_folder_local=None, str_pattern=None):
        """ Given a remote folder path in an SFTP server, find the last updated file in that folder str_folder_remote.
        Copy that file, appending a timestamp appended to its filename, to the target directory str_folder_local.

        If str_pattern (regex) is given, apply pattern as a filter to work with only a subset of files.
        This makes it quicker as we have to check the mtime of ALL eligible files.

        :return: A tuple containing (<str_file_originalname>, <str_filename_modified_full_path>), or (None, None) if file pattern not found.
        """
        # CREATE SFTP CONNECTION TO REMOTE SERVER #
        str_host = self.config['sftp']['sftp_server']
        str_userid = self.config['sftp']['userid']
        str_pw = self.config['sftp']['password']

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=str_host, username=str_userid, password=str_pw, cnopts=cnopts)
        srv.cwd(str_folder_remote)  # Change current working dir to here.

        if str_folder_remote is not None:
            l_files = srv.listdir(str_folder_remote)  # Get all filenames in the directory.

            # Apply filter to filename list #
            if str_pattern is not None:
                l_files = [f for f in l_files if re.search(str_pattern, f)]
                if len(l_files) == 0:
                    srv.close()
                    return (None, None, None)  # Let the calling method handle error logging instead.

            # Get the last modified file, from the filtered list #
            mtime_out = None  # Initialize outside the loop. Modified time of file.
            for file in l_files:
                if srv.isfile(file):  # Only process files, ignore directories.
                    mtime_curr = (srv.lstat(file)).st_mtime  # Larger float value means later file! Num of secs from 1 Jan 1970.

                    if mtime_out is None:
                        mtime_out = mtime_curr
                        str_file_out = file  # "file" refers to the filename alone.
                    else:
                        if mtime_curr > mtime_out:  # Keep only the most recent datetime value.
                            mtime_out = mtime_curr
                            str_file_out = file  # Overwrite only if timestamp is greater (later).

            # Now we have the last modified file. Modify its filename to include the timestamp, to use as local filename #
            str_file_out_local = os.path.splitext(str_file_out)[0] + '-' + dt.datetime.strftime(dt.datetime.today(),
                                                                                                '%Y%m%d_%H%M') + \
                                 os.path.splitext(str_file_out)[1]
            # Copy the file over.
            str_file_out_local_full = str_folder_local + str_file_out_local
            srv.get(str_file_out, str_file_out_local_full)
            srv.close()
            self.logger.info(f'Copied SFTP remote file {str_file_out} to local copy {str_file_out_local_full}')

            return (str_file_out, str_file_out_local_full)


    def get_sftp_filename_latest(self, str_folder_remote=None, str_pattern=None):
        """ Given a remote folder path in an SFTP server, find the last updated file in that folder str_folder_remote.
        If str_pattern (regex) is given, apply pattern as a filter to work with only a subset of files.
        This makes it quicker as we have to check the mtime of ALL eligible files.

        :return: Returns the file name, or None if file pattern not found.
        """
        # CREATE SFTP CONNECTION TO REMOTE SERVER #
        str_host = self.config['sftp']['sftp_server']
        str_userid = self.config['sftp']['userid']
        str_pw = self.config['sftp']['password']

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=str_host, username=str_userid, password=str_pw, cnopts=cnopts)
        srv.cwd(str_folder_remote)  # Change current working dir to here.

        if str_folder_remote is not None:
            l_files = srv.listdir(str_folder_remote)  # Get all filenames in the directory.

            # Apply filter to filename list #
            if str_pattern is not None:
                l_files = [f for f in l_files if re.search(str_pattern, f)]
                if len(l_files) == 0:
                    srv.close()
                    return None  # Let the calling method handle error logging instead.

            # Get the last modified file, from the filtered list #
            mtime_out = None  # Initialize outside the loop. Modified time of file.
            for file in l_files:
                if srv.isfile(file):  # Only process files, ignore directories.
                    mtime_curr = (
                        srv.lstat(file)).st_mtime  # Larger float value means later file! Num of secs from 1 Jan 1970.

                if mtime_out is None:
                    mtime_out = mtime_curr
                    str_file_out = file  # "file" refers to the filename alone.
                else:
                    if mtime_curr > mtime_out:  # Keep only the most recent datetime value.
                        mtime_out = mtime_curr
                        str_file_out = file  # Overwrite only if timestamp is greater (later).
            srv.close()
            return str_file_out


class OperaDataReader(DataReader):
    """ For reading Opera files, which are output by Vision (run on SQ's laptop), then copied to SFTP server.
    Files:
        - (DONE) Reservation Analytics 11Jul15 fwd 60 days.xlsx
        - (DONE) Reservation Analytics 11Jul15 fwd 61 days.xlsx
        - (DONE) Reservation Analytics RES -History.xlsx
        - (DONE) CAG.xlsx
        - (ON HOLD. NOT REALLY USED) Reservation Analytics 20 Jul15 Fwd Cancellations.xlsx
    """
    def __init__(self):
        super().__init__()
        self.SOURCE_NAME = self.config['data_sources']['opera']['source_name']
        self._init_logger(logger_name=self.SOURCE_NAME + '_datareader', source_name=self.SOURCE_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    @dec_err_handler(retries=0)  # Logs unforeseen exceptions. Put here, so that the next file load will be unaffected if earlier one fails.
    def load_otb(self, pattern=None, dt_snapshot_dt=None):
        """ Loads Opera OTB file which matches the pattern. There should only be 1 per day.
        Also used to load History/Actuals file, because it has a similar format.
        Important! Note the use of remove_duplicates() immediately after calling this! See that method for details.

        Note (19 Sep 2018): There might be an issue with unhandled duplication ("Reservation" tab) for stg_op_act_rev/stg_op_act_nonrev, ie: Opera Historical. These tables are not used in any of the subsequent datamarts.

        :param pattern: Regular expression, to use for filtering file name.
        :return: None
        """
        SOURCE = self.SOURCE_NAME  # 'opera'
        DEST = 'mysql'
        is_history_file = 'History' in pattern
        str_folder_remote = self.config['data_sources']['opera']['folder_remote']
        str_folder_local = self.config['data_sources']['opera']['folder_local']

        # Copy file to server first. We wrote the method to change the incoming file names to have the timestamp in them.
        str_fn, str_file_out_local_full = self.copy_sftp_file_latest(str_folder_remote=str_folder_remote, str_folder_local=str_folder_local, str_pattern=pattern)

        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=str_fn):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {str_fn}'
            raise MaxDataLoadException(str_err)

        self.logger.info('Reading file "{}". Local copy "{}"'.format(str_folder_remote + str_fn, str_file_out_local_full))

        # READ THE FILES #
        df_reservation = pd.read_excel(str_file_out_local_full, sheet_name='Reservation', keep_default_na=False, na_values=[' '])
        df_allotment = pd.read_excel(str_file_out_local_full, sheet_name='Allotment', keep_default_na=False, na_values=[' '])
        df_arr_depart = pd.read_excel(str_file_out_local_full, sheet_name='Arr Dept', keep_default_na=False, na_values=[' '])

        if '60' in pattern:  # Legacy. This specific tab is somehow named differently in each file!
            df_sales = pd.read_excel(str_file_out_local_full, sheet_name='Sales (for Corporate Production', keep_default_na=False, na_values=[' '])
        else:  # The "61" file here. For the "History" file as well.
            df_sales = pd.read_excel(str_file_out_local_full, sheet_name='Sales', keep_default_na=False, na_values=[' '])

        df_guest_profile = pd.read_excel(str_file_out_local_full, sheet_name='Guest profile', keep_default_na=False, na_values=[' '])
        df_no_of_guest = pd.read_excel(str_file_out_local_full, sheet_name='No of guest', keep_default_na=False, na_values=[' '])
        df_rev_vhac_vhb = pd.read_excel(str_file_out_local_full, sheet_name='Revenue VHAC VHB', keep_default_na=False, na_values=[' '])
        df_rev_vhc_vhk = pd.read_excel(str_file_out_local_full, sheet_name='Revenue VHC VHK', keep_default_na=False, na_values=[' '])
        df_rev_ohs_tqh = pd.read_excel(str_file_out_local_full, sheet_name='Revenue OHS TQH', keep_default_na=False, na_values=[' '])
        df_rev_oph_tes = pd.read_excel(str_file_out_local_full, sheet_name='Revenue OPH TES', keep_default_na=False, na_values=[' '])
        df_rev_rhs_amoy = pd.read_excel(str_file_out_local_full, sheet_name='Revenue RHS AMOY', keep_default_na=False, na_values=[' '])

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
                      'rev_sourcename', 'rev_eff_rate_amt', 'rev_proj_room_nts', 'rev_rmrev_extax',
                      'rev_food_rev_inctax', 'rev_oth_rev_inctax', 'rev_hurdle', 'rev_hurdle_override',
                      'rev_restriction_override']
        if is_history_file:  # The column name for the Historical/Act table is different from that of the OTB table.
            l_rev_cols = [x.replace('rev_proj_room_nts', 'rev_actual_room_nts') for x in l_rev_cols]

        df_rev_ohs_tqh.columns = l_rev_cols
        df_rev_oph_tes.columns = l_rev_cols
        df_rev_rhs_amoy.columns = l_rev_cols
        df_rev_vhac_vhb.columns = l_rev_cols
        df_rev_vhc_vhk.columns = l_rev_cols

        # Merge all the Revenue dfs. They were originally separated due to bandwidth limitations of the "Vision" extraction tool.
        df_merge_rev = pd.concat([df_rev_ohs_tqh, df_rev_oph_tes, df_rev_rhs_amoy, df_rev_vhac_vhb, df_rev_vhc_vhk])
        if dt_snapshot_dt is None:
            df_merge_rev['snapshot_dt'] = dt.datetime.now()
        else:
            df_merge_rev['snapshot_dt'] = dt_snapshot_dt

        # Merge all the non-Revenue dfs, using Reservation as the base. They are said to be of 1-to-1 relationship.
        l_dfs = [df_reservation, df_allotment, df_arr_depart, df_sales, df_guest_profile, df_no_of_guest]
        df_merge_nonrev = functools.reduce(lambda left, right: pd.merge(left, right, on=['confirmation_no', 'resort']), l_dfs)
        if dt_snapshot_dt is None:
            df_merge_nonrev['snapshot_dt'] = dt.datetime.now()
        else:
            df_merge_nonrev['snapshot_dt'] = dt_snapshot_dt

        # TYPE CONVERSIONS #
        # errors='coerce' is needed sometimes because of presence of blanks (no dob given). Blanks will thus be converted to NaT.
        # For such cases, no 'format' specified, because that causes every row to become NaT (dunno what's the format in Excel).
        if is_history_file:  # Only the "History" file has this field.
            df_merge_nonrev['res_business_date'] = pd.to_datetime(df_merge_nonrev['res_business_date'], format='%d/%m/%Y')
        df_merge_nonrev['gp_dob'] = pd.to_datetime(df_merge_nonrev['gp_dob'], errors='coerce')
        df_merge_nonrev['res_reserv_date'] = pd.to_datetime(df_merge_nonrev['res_reserv_date'], format='%d/%m/%Y')
        df_merge_nonrev['res_cancel_date'] = pd.to_datetime(df_merge_nonrev['res_cancel_date'], errors='coerce')
        df_merge_nonrev['arr_arrival_dt'] = pd.to_datetime(df_merge_nonrev['arr_arrival_dt'], format='%d/%m/%Y')
        df_merge_nonrev['arr_departure_dt'] = pd.to_datetime(df_merge_nonrev['arr_departure_dt'], format='%d/%m/%Y')
        df_merge_rev['business_dt'] = pd.to_datetime(df_merge_rev['business_dt'], format='%d/%m/%Y')

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
        #os.remove(str_fn_dst)  # Thought: Might want to keep the copied files, to re-create error situations.

    @dec_err_handler(retries=0)  # Logs unforeseen exceptions. Put here, so that the next file load will be unaffected if earlier one fails.
    def load_cag(self, pattern=None, dt_snapshot_dt=None):
        """ Loads Opera CAG file which matches the pattern. There should only be 1 per day.
        :param pattern: Regular expression, to use for filtering file name.
        :return: None
        """
        SOURCE = self.SOURCE_NAME  # 'opera'
        DEST = 'mysql'

        str_folder_remote = self.config['data_sources']['opera']['folder_remote']
        str_folder_local = self.config['data_sources']['opera']['folder_local']

        # Copy file to server first. We wrote the method to change the incoming file names to have the timestamp in them.
        str_fn, str_file_out_local_full = self.copy_sftp_file_latest(str_folder_remote=str_folder_remote, str_folder_local=str_folder_local, str_pattern=pattern)

        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=str_fn):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {str_fn}'
            raise MaxDataLoadException(str_err)

        self.logger.info('Reading file "{}". Local copy "{}"'.format(str_folder_remote + str_fn, str_file_out_local_full))

        # READ THE FILE #
        df_allot = pd.read_excel(str_file_out_local_full, sheet_name='Allotment ', keep_default_na=False, na_values=[' '])
        df_allotgrp = pd.read_excel(str_file_out_local_full, sheet_name='Allotment +Group', keep_default_na=False, na_values=[' '], usecols='A:U')

        df_allot.columns = df_allot.columns[1:].insert(0, 'resort')  # Excel formula causes first column to be read in as 'nan'.
        df_allotgrp.columns = df_allotgrp.columns[1:].insert(0, 'resort')

        # Remove unnecessary characters from column names, and lowercase all columns.
        l_cols = df_allot.columns.str.replace('[()/.]', '')  # Regex
        l_cols = l_cols.str.replace(r' ', '_')
        l_cols = l_cols.str.replace(r'__', '_')  # Replace resultant double underscore with only single underscore. Happens only for df_allot.
        l_cols = l_cols.str.lower()
        df_allot.columns = l_cols

        l_cols = df_allotgrp.columns.str.replace('[()/.?]', '')  # Regex
        l_cols = l_cols.str.replace(r' ', '_')
        l_cols = l_cols.str.lower()
        df_allotgrp.columns = l_cols

        # Keep only rows where the status is Definite, Tentative, or Allocated (to Wholesaler). Actual (ACT) and CXL (Cancelled) are irrelevant and hence omitted.
        df_allot = df_allot[df_allot['booking_status_code'].isin(['DEF', 'TEN', 'ALLT'])]
        df_merge = df_allot.merge(df_allotgrp, how='inner',
                                  left_on=['resort', 'allotment_date', 'allotment_booking_description', 'label'],
                                  right_on=['resort', 'considered_date', 'company', 'label'])  # stay_date = 'allotment_date' = 'considered_date'
        # Tidying up #
        # Drop unwanted columns.
        df_merge.columns = df_merge.columns.str.replace('-', '_')

        df_merge.drop(labels=['begin_date', 'end_date', 'original_begin_date', 'original_end_date', 'date_opened_for_pickup',
                              'cancellation_date', 'decision_date_for_rooms', 'follow_up_date_for_rooms',
                              'decision_date_for_catering', 'to_sell', 'released', 'available', 'aggregate_prospective_occupancy',
                              'aggregate_pickup_occupancy', 'block_or_individual_', 'reservation_inventory_type',
                              'considered_date', 'origin_code', 'country_code', 'company'], axis=1, inplace=True)
        df_merge = df_merge[df_merge.columns[~df_merge.columns.str.endswith('_y')]]  # Keep only columns with no "_y" suffix.
        df_merge.columns = df_merge.columns.str.replace('_x$', '')  # For the column names with the "_x" suffix, remove the suffix part.

        if dt_snapshot_dt is None:
            df_merge['snapshot_dt'] = dt.datetime.now()
        else:
            df_merge['snapshot_dt'] = dt_snapshot_dt

        # WRITE TO DATABASE #
        self.logger.info('Loading file contents to data warehouse.')
        df_merge.to_sql('stg_op_cag', con=self.db_conn, index=False, if_exists='append')  # CAG: Cancellations, Allotments, Groups.

        # LOG DATALOAD #
        self.logger.info('Logged data load activity to system log table.')
        self.log_dataload('opera', 'mysql', str_fn)

        # TODO Uncomment this line when system has stabilized, to delete temp files.
        # Remove the temp file copy.
        #os.remove(str_fn_dst)  # Thought: Might want to keep the copied files, to re-create error situations.

    def load(self, dt_snapshot_dt=None):
        """ Loads data from a related cluster of data sources.
        If dt_snapshot_dt is given, will use this as the snapshot_dt, instead of the current run date.
        :return: N/A
        """
        # Check if any of the 3 files (60/61/CAG) files are missing, do not load anything Opera at all. This wil allow copy_last_snapshot_dt_dataset() functionality to be triggered. This defends against the scenario where only 60 or 61 file is missing. #
        str_folder_remote = self.config['data_sources']['opera']['folder_remote']
        str_fn1 = self.get_sftp_filename_latest(str_folder_remote=str_folder_remote, str_pattern='60 days.xlsx$')
        str_fn2 = self.get_sftp_filename_latest(str_folder_remote=str_folder_remote, str_pattern='61 days.xlsx$')
        str_fn3 = self.get_sftp_filename_latest(str_folder_remote=str_folder_remote, str_pattern='CAG.xlsx$')

        if (str_fn1 is None) | (str_fn2 is None) | (str_fn3 is None):
            self.logger.error('[{}] Opera data loading terminated. Ensure that 60/61/CAG files are all present.'.format(self.SOURCE_NAME))
        else:
            self.load_otb(pattern='60 days.xlsx$', dt_snapshot_dt=None)  # OTB 0-60 days onwards. Currently always picks the last modified file with this file name.
            self.load_otb(pattern='61 days.xlsx$', dt_snapshot_dt=None)  # OTB 61 days onwards.
            self.load_cag(pattern='CAG.xlsx$', dt_snapshot_dt=None)  # CAG (Cancellations, Allocations, Groups)
            self.load_otb(pattern='History.xlsx$', dt_snapshot_dt=None)  # History (aka: Actuals)
            self.remove_duplicates()  # Eliminates duplicates in stg_op_otb_nonrev, potentially caused by "60" and "61" files. Uses today() by default.

    def remove_duplicates(self, dt_snapshot_dt=dt.datetime.today()):
        """ Will operate only on Table: stg_op_otb_nonrev. Not relevant elsewhere.
        There are situations whereby if a guest stays for multiple stay_dates such that he crosses the boundary of
        the "60" and "61" files, the Table will end up containing duplicates, because the nonrev part does not contain
        stay_date as a column!

        This method is capable of processing for either 1) The given snapshot_dt, or 2) For the entire table (if no snapshot_dt is given).
        :return: NA
        """
        # Subset of ALL columns, EXCEPT for snapshot_dt.
        l_subset = ['confirmation_no', 'resort', 'res_reserv_status', 'res_bkdroom_ct_lbl', 'res_roomty_lbl',
                    'res_reserv_date', 'res_cancel_date', 'res_pay_method', 'res_credcard_ty', 'allot_allot_book_cde',
                    'allot_allot_book_desc', 'arr_arrival_dt', 'arr_departure_dt', 'arr_arrival_time',
                    'arr_departure_time', 'sale_comp_name', 'sale_zip_code', 'sale_sales_rep', 'sale_sales_rep_code',
                    'sale_industry_code', 'sale_trav_agent_name', 'sale_sales_rep1', 'sale_sales_rep_code1',
                    'sale_all_owner', 'gp_guest_ctry_cde', 'gp_nationality', 'gp_origin_code', 'gp_guest_lastname',
                    'gp_guest_firstname', 'gp_guest_title_code', 'gp_spcl_req_desc', 'gp_email', 'gp_dob',
                    'gp_vip_status_desc', 'ng_adults', 'ng_children', 'ng_extra_beds', 'ng_cribs', 'snapshot_dt2']

        if dt_snapshot_dt is None:
            # PROCESS ENTIRE TABLE, IF EXPLICITLY SET TO NONE #
            str_sql = """
            SELECT * FROM stg_op_otb_nonrev
            """
            df = pd.read_sql(str_sql, self.db_conn)
            df2 = df  # So that can use df2, without re-reading the 3.8M row table again!
            df2['snapshot_dt2'] = df2['snapshot_dt'].dt.date  # Get only the date component
            df_not_dup = df2[~df2.duplicated(subset=l_subset)]  # Duplicates marked as True. Keep the non-dups.
            df_not_dup.drop(labels=['snapshot_dt2'], axis=1, inplace=True)  # Eliminate the auxilliary column.

            if len(df) != len(df_not_dup):  # Do so only if the number of records is different. Same means no change.
                df_not_dup.to_sql('stg_op_otb_nonrev', self.db_conn, index=False, if_exists='replace')
                self.logger.info('[remove_duplicates] Replaced ALL contents of Table: stg_op_otb_nonrev')
        else:
            # PROCESS ONLY FOR THE GIVEN snapshot_dt #
            str_date_from, str_date_to = feh.utils.split_date(dt_snapshot_dt)

            str_sql = """
            SELECT * FROM stg_op_otb_nonrev
            WHERE snapshot_dt >='{}' AND snapshot_dt <'{}'
            """.format(str_date_from, str_date_to)

            df = pd.read_sql(str_sql, self.db_conn)  # A subset of the table's contents; only for 1 snapshot_dt.
            df['snapshot_dt2'] = df['snapshot_dt'].dt.date  # Get only the date component
            df_not_dup = df[~df.duplicated(subset=l_subset)]  # Duplicates marked as True. Keep the non-dups.
            df_not_dup.drop(labels=['snapshot_dt2'], axis=1, inplace=True)  # Eliminate the auxilliary column.

            # DELETE RECORDS FOR THAT snapshot_dt #
            if len(df) != len(df_not_dup):  # Do so only if the number of records is different. Same means no change.
                str_sql = """
                DELETE FROM stg_op_otb_nonrev
                WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
                """.format(str_date_from, str_date_to)

                pd.io.sql.execute(str_sql, self.db_conn)
                self.logger.info('[remove_duplicates] Deleted all records for snapshot_dt: {}'.format(str_date_from))

                # PUT BACK THE DE-DUPLICATED RECORDS FOR THAT snapshot_dt #
                df_not_dup.to_sql('stg_op_otb_nonrev', self.db_conn, index=False, if_exists='append')
                self.logger.info('[remove_duplicates] Restored de-duplicated records back to table.')


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

        # BUFFER HotelIDs and CompsetID INTO MEMORY, EXCEPT FOR OSKL #
        str_sql = """
        SELECT SubscriptionID, HotelID, HotelName, CompetitorID, CompetitorName FROM stg_otai_hotels 
        WHERE hotel_category = 'hotel'
        AND HotelID <> '1629556'
        """  # Exclude OSKL.
        try:
            df = pd.read_sql(str_sql, self.db_conn)  # Read HotelIDs/CompsetIDs into buffer for future querying.
        except Exception:  # pymysql.err.InterfaceError thrown if table does not exist. Make API call only if there's no data. More resilient.
            self.load_hotels()
            df = pd.read_sql(str_sql, self.db_conn)
            self.df_hotels = df  # Class attribute.

        if len(df) != 0:  # Table might exist, but have no data (ie: truncated).
            self.df_hotels = df
        else:  # Make API call only if there's no data. More resilient.
            self.load_hotels()
            df = pd.read_sql(str_sql, self.db_conn)
            self.df_hotels = df  # Class attribute.

        # List of UNIQUE SubscriptionIDs. For use in future calls (eg: self.load_rates() ).
        self.l_sub_id = set(self.df_hotels['SubscriptionID'].tolist())

    def __del__(self):
        super().__del__()
        self._free_logger()

    @dec_err_handler(retries=0)
    def load_hotels(self, dt_snapshot_dt=None):
        """ Loads Hotels and Compset IDs to data warehouse. We need the IDs for further queries (eg: rates).
        As the list of hotels and compset can change without notice, we will load this once per day, overwriting the existing table.

        Note1: OSKL is a hotel. However, in OTAIDataReader.__init__(), we exclude it from l_sub_id because we do not want to include this hotel in the Rates API calls. This is because the price ranking is entirely SG oriented.

        Note2: In OTAI's API, URL parameters are written in lower camel case (eg: subscriptionId), and are case-sensitive!
        Conversely, format=csv header names are written in upper camel case. And they differ also from format=json header names. Go figure...
        """
        self.logger.info('Hotel CompSet: Reading from API.')
        res = requests.get(self.BASE_URL + 'hotels', params={'token': self.API_TOKEN, 'format': 'csv'})  # Better to use requests module. Can check for "requests.ok" -> HTTP 200.
        df_hotels = pd.read_csv(io.StringIO(res.content.decode('utf-8')))
        #df_hotels.columns = ['hotel_id', 'hotel_name', 'hotel_stars', 'comp_id', 'comp_name', 'comp_stars', 'compset_id', 'compset_name']

        # New column. Label the dataset with known Hotels; HotelIds not in the list are deemed to be SRs.
        # Note that this labeling checks against HotelID; it is with reference to FEH's hotels and SRs only.
        l_hotelid = [25002, 25105, 25106, 25107, 25109, 25119, 25167, 296976, 359549, 613872, 1663218, 1629556]  # 11 hotels + OSKL (1629556). OSKL is excluded from the Rates API calls.
        df_hotels['hotel_category'] = 'sr'  # default value.
        df_hotels['hotel_category'][df_hotels['HotelID'].isin(l_hotelid)] = 'hotel'
        if dt_snapshot_dt is None:
            df_hotels['snapshot_dt'] = dt.datetime.now()
        else:
            df_hotels['snapshot_dt'] = dt_snapshot_dt

        # Keep only the CompsetID = 1 (ie: CompsetName = 'App Primary'). Ignore secondary compset.
        df_hotels = df_hotels[df_hotels['CompsetID'] == 1]
        df_hotels['HotelID'] = df_hotels['HotelID'].astype(str)  # Standardize and convert all ID-type fields to string.
        df_hotels['SubscriptionID'] = df_hotels['SubscriptionID'].astype(str)
        df_hotels['CompetitorID'] = df_hotels['CompetitorID'].astype(str)
        df_hotels['CompsetID'] = df_hotels['CompsetID'].astype(str)

        # WRITE TO DATABASE #
        self.logger.info('Hotel CompSet: Loading to data warehouse.')
        df_hotels.to_sql('stg_otai_hotels', self.db_conn, index=False, if_exists='replace')
        # Note that there is no need to write to system log, because no process is checking for this. You can see last load from the snapshot_dt column.

    def get_rates_hotel(self, str_sub_id=None, format='csv', ota='bookingdotcom'):
        """ Calls the OTAI API to get the competitor rates, for a given FEH SubscriptionID.
        Defaults: los: 1, persons:2, mealType: nofilter (lowest), shopLength: 90 (max: 365), changeDays: None (max: 3 values, max: 30 days), compsetIds=1
        Not using parameter: changeDays (we will calculate it ourselves).
        Note: You should call the API using only our own HotelIDs, else you'll get a HTTP 401.
        :param str_sub_id:
        :param format:
        :param ota:
        :return:
        """
        params = {'token': self.API_TOKEN,
                  'format': format,
                  'subscriptionId': str_sub_id,  # parameter names are case sensitive.
                  'ota': ota
                  }
        res = requests.get(self.BASE_URL + 'rates', params=params)

        if res.status_code == 200:  # HTTP 200
            df = pd.read_csv(io.StringIO(res.content.decode('utf-8')))
            df['ArrivalDate'] = pd.to_datetime(df['ArrivalDate'])
            df['ExtractDateTime'] = pd.to_datetime(df['ExtractDateTime'])
            df['ota'] = ota  # include the OTA as well, as this does not form part of the data set from the API.
            return df
        elif res.status_code == 429:  # 429 Too Many Requests. API calls are rate-limited to 120 requests/min per token.
            str_err = 'Hotel Rates: HTTP 429 Too Many Requests'
            raise Exception(str_err)
        else:
            str_err = 'Unknown exception. HTTP Request status code: {}'.format(res.status_code)
            raise Exception(str_err)

    @dec_err_handler(retries=0)
    def load_rates(self, dt_snapshot_dt=None):
        SOURCE = self.SOURCE_NAME  # 'otai'
        DEST = 'mysql'
        FILE = 'rates'
        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=FILE):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {FILE}'
            raise MaxDataLoadException(str_err)

        df_all = DataFrame()  # accumulator

        self.logger.info('Hotel Rates: Reading from API.')
        for sub_id in self.l_sub_id:  # Get the Rates exactly ONCE for each SubscriptionID. The list has unique IDs already.
            df = self.get_rates_hotel(str_sub_id=str(sub_id))
            df_all = df_all.append(df, ignore_index=True)

        # De-duplicate rates in df_all. Duplicates may exist because the same hotel is a competitor for 2 (or more) of our hotels.
        # 16 Mar 2018: It may be the case that if for the same stay_date/hotelid, OTAI returns 2 rows for a hotel, each row being different ROOM TYPES at the same price!
        # Hence the drop_duplicates() below is using the combined key of ['HotelID', 'ArrivalDate', 'Value'].
        df_all.drop_duplicates(subset=['HotelID', 'ArrivalDate', 'Value'], inplace=True)

        df_all['HotelID'] = df_all['HotelID'].astype(str)  # Standardize and convert all ID-type fields to string.

        self.logger.info('Hotel Rates: Loading to data warehouse.')
        if dt_snapshot_dt is None:
            df_all['snapshot_dt'] = dt.datetime.now()  # Add a timestamp when the data was loaded.
        else:
            df_all['snapshot_dt'] = dt_snapshot_dt

        # WRITE TO DATABASE #
        df_all.to_sql('stg_otai_rates', self.db_conn, index=False, if_exists='append')

        # LOG DATALOAD #
        self.logger.info('Hotel Rates: Logged data load activity to system log table.')
        self.log_dataload(source='otai', dest='mysql', file='rates')

    def load(self, dt_snapshot_dt=None):
        """ Loads data from a related cluster of data sources.
        If dt_snapshot_dt is given, will use this as the snapshot_dt, instead of the current run date.
        """
        self.load_hotels(dt_snapshot_dt=None)  # This must run first, in case the CompSet has been changed.
        self.load_rates(dt_snapshot_dt=None)


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
    def load_projected_and_otb(self, pattern=None, type=None, dt_snapshot_dt=None):
        """ Loads FWK Projected filename which matches the given pattern. There should only be 1 per day.
        Also used to load FWK OTB file, because it has a similar format.
        :param pattern: Regular expression, to use for filtering file name.
        :return: None
        """
        SOURCE = self.SOURCE_NAME  #'fwk'
        DEST = 'mysql'
        str_folder_remote = self.config['data_sources']['fwk']['folder_remote']
        str_folder_local = self.config['data_sources']['fwk']['folder_local']

        # Copy file to server first. We wrote the method to change the incoming file names to have the timestamp in them.
        str_fn, str_file_out_local_full = self.copy_sftp_file_latest(str_folder_remote=str_folder_remote, str_folder_local=str_folder_local, str_pattern=pattern)

        # Check using str_fn if file has already been loaded before. If yes, terminate processing.
        if self.has_exceeded_dataload_freq(source=SOURCE, dest=DEST, file=str_fn):
            str_err = f'Maximum data load frequency exceeded. Source: {SOURCE}, Dest: {DEST}, File: {str_fn}'
            raise MaxDataLoadException(str_err)

        # READ THE LOCAL COPY OF THE FILES #
        df = pd.read_csv(str_file_out_local_full)
        df = df.iloc[:, :-1]  # Drop the last column. Blank.

        # Typecasting
        df['year'] = df['year'].astype(str)
        df['periodFrom'] = pd.to_datetime(df['periodFrom'], format='%d/%m/%Y')
        df['periodTo'] = pd.to_datetime(df['periodTo'], format='%d/%m/%Y')
        if dt_snapshot_dt is None:
            df['snapshot_dt'] = dt.datetime.now()  # Add timestamp as handle.
        else:
            df['snapshot_dt'] = dt_snapshot_dt

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

    def load(self, dt_snapshot_dt=None):
        """ Loads data from a related cluster of data sources.
        If dt_snapshot_dt is given, will use this as the snapshot_dt, instead of the current run date.
        """
        self.load_projected_and_otb(pattern='^FWK_PROJ.+csv$', type='projected', dt_snapshot_dt=None)  # eg: FWK_PROJ_29JAN2018.csv
        self.load_projected_and_otb(pattern='^FWK.+SCREEN1.csv$', type='otb', dt_snapshot_dt=None)  # eg: FWK_29JAN2018_SCREEN1.csv


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

    @dec_err_handler(retries=5)
    def load_forecast(self, dt_snapshot_dt=None):
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
        # Path to chromedriver executable. Get latest versions from https://sites.google.com/a/chromium.org/chromedriver/downloads
        str_fn_chromedriver = os.path.join(self.config['global']['global_bin'], 'chromedriver_2.37.exe')
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
        time.sleep(2)  # Just in case. The below statement *might* occur before the above statement completes, leading to error.
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
        str_dl_folder = os.path.join(os.getenv('USERPROFILE'), 'Downloads')  # eg: 'Downloads' folder of the user.
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
        if dt_snapshot_dt is None:
            df['snapshot_dt'] = dt.datetime.now()
        else:
            df['snapshot_dt'] = dt_snapshot_dt
        df_forecast = df

        self.logger.info('Writing to database.')
        df_forecast.to_sql('stg_ezrms_forecast', self.db_conn, index=False, if_exists='append')

        # LOG DATALOAD #
        self.logger.info('Logged data load activity to system log table.')
        self.log_dataload('ezrms', 'mysql', FILE)

        driver.close()  # Close the browser
        os.remove(str_fn_with_path)  # Delete the TSV file.

    def load(self, dt_snapshot_dt=None):
        """ Loads data from a related cluster of data sources.
        If dt_snapshot_dt is given, will use this as the snapshot_dt, instead of the current run date.
        """
        self.load_forecast(dt_snapshot_dt=None)  # EzRMS's forecast of what the occupancies. Tightly coupled with the EzRMS report used.


class EloquaB2CDataReader(DataReader):
    def __init__(self):
        super().__init__()
        self.SOURCE_NAME = self.config['data_sources']['eloqua_b2c']['source_name']
        self._init_logger(logger_name=self.SOURCE_NAME + '_datareader', source_name=self.SOURCE_NAME)

        self.COMPANY = self.config['data_sources']['eloqua_b2c']['company']
        self.USERID = self.config['data_sources']['eloqua_b2c']['userid']
        self.PASSWORD = self.config['data_sources']['eloqua_b2c']['password']
        self.bulk = Bulk(company=self.COMPANY, username=self.USERID, password=self.PASSWORD)

    def __del__(self):
        super().__del__()
        self._free_logger()

    def get_activity_data(self, str_act_type=None, start_date=None, end_date=None):
        """
        Given a valid Eloqua activity, retrieve the data from Eloqua, where ActivityDate is between start_date (inclusive) and end_date (exclusive).
        :param str_act_type: Eloqua activity types. Valid options: EmailSend, EmailOpen, EmailClickthrough, Subscribe, Unsubscribe, Bounceback, FormSubmit, PageView, WebVisit.
        :param start_date: datetime object representing the start_date of the selection.
        :param end_date: datetime object representing the end date of the selection. Note that selection is EXCLUSIVE of end date itself!
        :return: DataFrame object. Returns empty DataFrame if no data retrieved.
        """
        # Ensure all parameters are provided.
        if str_act_type == None or start_date == None or end_date == None:
            raise Exception('Please provide valid values for {} and {} and {} and {}'
                            .format('bulk', str_act_type, start_date, end_date))

        self.bulk.exports(elq_object='activities', act_type=str_act_type)
        fields = self.bulk.get_fields(elq_object='activities', act_type=str_act_type)
        self.bulk.add_fields(list(DataFrame(fields)['name']))

        # bulk.filter_equal(field='ActivityType', value=str_act_type)  # Required for Activities
        self.bulk.filter_date(field='ActivityDate', start=start_date, end=end_date)  # Non-inclusive of end date!
        self.bulk.create_def('export_activities_' + str_act_type)  # send export info to Eloqua
        self.bulk.sync()  # Move data to staging area in Eloqua.
        df = DataFrame(self.bulk.get_export_data())  # Convert returned JSON values to DataFrame.

        # Typecasting
        df['ActivityDate'] = pd.to_datetime(df['ActivityDate'], errors='coerce')

        if len(df) == 0:
            return DataFrame()  # Returns empty DataFrame if no data. For use in concatenation.
        else:
            return df

    def load_activity(self, str_act_type=None, str_dt_from=None, str_dt_to=None):
        """
        Calls get_activity_data() to iteratively (day-by-day) obtain all activity data, for the given date range.
        This attempts to bypass the problem of Eloqua taking too long to return data, if the selection date range is too wide.
        Reminder: The data is EXCLUSIVE of str_to_dt itself!
        :param str_act_type:
        :param str_dt_from:
        :param str_dt_to:
        :return: NA
        """
        # Typecasting strings to datetime.
        dt_from = pd.to_datetime(str_dt_from)
        dt_to = pd.to_datetime(str_dt_to)

        if not dt_to > dt_from:
            raise Exception('dt_to must be at least 1 day larger than dt_from. Eloqua API constraint.')

        dt_ptr_from = dt_from
        dt_ptr_to = dt_from + dt.timedelta(days=1)

        while dt_ptr_to <= dt_to:
            self.logger.info('Obtaining {} data for start_date={}, end_date={}'
                             .format(str_act_type, str(dt_ptr_from.date()), str(dt_ptr_to.date())))
            self.bulk.job['filters'] = []  # Clear filters (for ActivityDate and ActivityType). So won't duplicate and throw Eloqua API error.

            df = self.get_activity_data(str_act_type, dt_ptr_from, dt_ptr_to)

            str_db_tab_name = 'stg_elq_b2c_act_' + str_act_type.lower()  # Convention: Tab name will be derived from ActivityType.
            # Ref => https: // docs.oracle.com / cloud / latest / marketingcs_gs / OMCAB / index.html  # Developers/BulkAPI/Reference/activity-fields.htm%3FTocPath%3DBulk%2520API%7CReference%7C_____7
            # WRITE TO DB #
            df.to_sql(name=str_db_tab_name, con=self.db_conn, if_exists='append', index=False)

            dt_ptr_from = dt_ptr_to  # Increment dt_ptr_start_date by 1
            dt_ptr_to += dt.timedelta(days=1)  # Increment dt_ptr_end_date by 1

    def load_activity_bounceback(self, str_act_type=None, str_dt_from=None, str_dt_to=None):
        """
        Variant of load_activity(). There are few bouncebacks, so might be better to not call day-by-day.
        :param str_act_type:
        :param str_from_dt:
        :param str_to_dt:
        :return: NA
        """
        # Typecasting strings to datetime.
        dt_from = pd.to_datetime(str_dt_from)
        dt_to = pd.to_datetime(str_dt_to)

        if not dt_to > dt_from:
            raise Exception('dt_to must be at least 1 day larger than dt_from. Eloqua API constraint.')
        self.logger.info('Obtaining {} data for start_date={}, end_date={}'
                         .format(str_act_type, str(dt_from.date()), str(dt_to.date())))
        self.bulk.job['filters'] = []  # Clear filters (for ActivityDate and ActivityType). So won't duplicate and throw Eloqua API error.
        df = self.get_activity_data(str_act_type, dt_from, dt_to)  # Not for single day, but for a date range.
        str_db_tab_name = 'stg_elq_b2c_act_' + str_act_type.lower()  # Convention: Tab name will be derived from ActivityType.
        # WRITE TO DB #
        df.to_sql(name=str_db_tab_name, con=self.db_conn, if_exists='append', index=False)

    def load(self):
        pass