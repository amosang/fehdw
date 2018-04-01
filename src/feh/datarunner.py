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
from pandas import DataFrame, Series
import feh.utils
from feh.utils import dec_err_handler, get_files, MaxDataLoadException, split_date


class DataRunner(object):
    """ This general class contains the methods to do with processing data from the raw state into an intermediate state, to place in data marts.
    Data mart tables will have a "dm1" prefix. The "1" signifies that this is level 1 of processing. There may be subsequent levels.
    This class itself contains only generic methods such as those for logging. It is meant to be inherited.
    Its sub-classes will contain the methods which contain the actual logic for further data processing.
    """
    config = ConfigObj('C:/fehdw/src/fehdw.conf')

    def __init__(self):
        # CREATE CONNECTION TO DB #  All data movements involve the database, so this is very convenient to have.
        str_host = self.config['data_sources']['mysql']['host']
        str_userid = self.config['data_sources']['mysql']['userid']
        str_password = self.config['data_sources']['mysql']['password']
        str_db = self.config['data_sources']['mysql']['db']
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_db}?charset=utf8mb4'
        engine = sqlalchemy.create_engine(str_conn_mysql, echo=False)
        self.conn_fehdw = engine.connect()

        self.APP_NAME = self.config['datarunner']['datarunner']['app_name']
        self._init_logger(logger_name='datarunner', app_name=self.APP_NAME)

    def __del__(self):
        self.conn_fehdw.close()
        self._free_logger()

    def _init_logger(self, logger_name='datarunner', app_name=None):
        self.logger = logging.getLogger(logger_name)  # A specific id for the Logger class use only.
        self.logger.setLevel(logging.INFO)  # By default, logging will start at 'WARNING' unless we tell it otherwise.

        if self.logger.hasHandlers():  # Clear existing handlers, else will have duplicate logging messages.
            self.logger.handlers.clear()

        # LOG TO GLOBAL LOG FILE #
        str_fn_logger_global = os.path.join(self.config['global']['global_root_folder'], self.config['global']['global_log'])
        fh_logger_global = logging.FileHandler(str_fn_logger_global)

        # Add global handler.
        str_format_global = f'[%(asctime)s]-[%(levelname)s]-[{logger_name}] %(message)s'
        fh_logger_global.setFormatter(logging.Formatter(str_format_global))
        self.logger.addHandler(fh_logger_global)

        # Add local handler.
        str_fn_logger = os.path.join(self.config['global']['global_root_folder'], self.config['datarunner'][app_name]['logfile'])
        fh_logger = logging.FileHandler(str_fn_logger)
        str_format = '[%(asctime)s]-[%(levelname)s]-%(message)s'
        fh_logger.setFormatter(logging.Formatter(str_format))
        self.logger.addHandler(fh_logger)

    def _free_logger(self):
        """ Frees up all file handlers. Method is to be called on __del__().
        :return:
        """
        # Logging. Close all file handlers to release the lock on the open files.
        handlers = self.logger.handlers[:]  # https://stackoverflow.com/questions/15435652/python-does-not-release-filehandles-to-logfile
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)

    def _generic_run_all(self, run_id, l_data_src_tabs, str_func_name, i_dt_from_offset=0, str_dt_from=None, str_dt_to=None):
        """ Generic method for repeated calling of a given method.
        Reduces the amount of repeated code typed, as all the "*_all" methods were observed to have highly similar code, which was extracted out here.
        :param run_id: This field comes from the calling "*_all" method.
        :param l_data_src_tabs: Assumes that these tables have "snapshot_dt" column. Will take the smallest intersection of all tables.
        :param str_func_name: The method name to call repeatedly. Assumes that this method is in the current class.
        :param i_dt_from_offset: Number of days by which to shift "dt_from" later in time.
        :param str_dt_from: This field comes from the calling "*_all" method.
        :param str_dt_to: This field comes from the calling "*_all" method.
        :return: NA
        """
        dt_from = None
        dt_to = None

        # Get the intersection of snapshot_dts, if multiple tables are involved #
        for tab in l_data_src_tabs:
            # Setting upper and lower rational bounds on dt_from and dt_to, to avoid unnecessary processing #
            str_sql = """
            SELECT MIN(snapshot_dt) AS snapshot_dt_min, MAX(snapshot_dt) AS snapshot_dt_max FROM {}
            """.format(tab)
            sr = pd.read_sql(str_sql, self.conn_fehdw).loc[0]
            dt_from_temp = sr['snapshot_dt_min'] + dt.timedelta(days=i_dt_from_offset)  # Shift by N days, because we want a look-back of at least N days.
            dt_to_temp = sr['snapshot_dt_max']

            if dt_from is None:
                dt_from = dt_from_temp
            else:
                if dt_from_temp > dt_from:
                    dt_from = dt_from_temp  # for "from", we want the later date.

            if dt_to is None:
                dt_to = dt_to_temp
            else:
                if dt_to_temp < dt_to:
                    dt_to = dt_to_temp  # for "to", we want the earlier date.

        # Replace the bounds, only if so desired. Can selectively choose to overwrite only dt_from OR dt_to.
        if str_dt_from is not None:
            dt_from = pd.to_datetime(str_dt_from)
        if str_dt_to is not None:
            dt_to = pd.to_datetime(str_dt_to)

        self.logger.info('[{}] Processing data for period: {} to {}'.format(run_id, str(dt_from.date()), str(dt_to.date())))

        meth = getattr(self.__class__, str_func_name)  # From the method name, get the method.
        for d in range((dt_to.normalize() - dt_from.normalize()).days + 1):  # +1 to make it inclusive of the dt_to.
            meth(self, dt_date=dt_from + dt.timedelta(days=d))

    def has_been_loaded(self, source, dest, file, dt_date=dt.datetime.today()):
        """ DEPRECATED.
        20 Mar 2018: If a data load fails, there will already be an alert. The admin should rectify the data load
        issue, then run the "*_all" method, to retroactively generate the data mart records.

        Duplicate runs on the same snapshot_dt is already taken care of (through SKIPPED and has_exceeded_datarun_freq() ).
        Only snapshot_dt that has not been run before, will be processed.
        Therefore, for ease of use, we should avoid putting more constraints in the "proc*" methods.

        Check if a data source has been loaded for a given snapshot_dt (usually the value is current date).
        For checking if a dependent data feed has been loaded, before doing data runs on it.
        :param source:
        :param dest:
        :param file:
        :param dt_date: Usually is the current date.
        :return: NA
        """
        str_date_from, str_date_to = split_date(dt_date)

        # Get number of logged data runs already run within the same day (KEY: run_id).
        str_sql = """
        SELECT * FROM sys_log_dataload
        WHERE source = '{}'
        AND dest = '{}'
        AND file = '{}'
        AND timestamp >= '{}' AND timestamp < '{}' 
        """.format(source, dest, file, str_date_from, str_date_to)
        df = pd.read_sql(str_sql, self.conn_fehdw)
        i_log_count = len(df)  # Number of existing log entries.
        if i_log_count > 0:
            return True
        else:
            return False

    def log_datarun(self, run_id, str_snapshot_dt):
        """ For logging data runs. Timestamp used is current time.
        :param run_id:
        :param str_snapshot_dt:
        :return:
        """
        try:
            str_sql = """INSERT INTO sys_log_datarun (run_id, snapshot_dt, timestamp) VALUES ('{}', '{}', '{}')
            """.format(run_id, str_snapshot_dt, dt.datetime.now())
            pd.io.sql.execute(str_sql, self.conn_fehdw)
        except Exception:  # sqlalchemy.exc.ProgrammingError
            # If Exception, that can only mean that the table is not there. Create the table, and do again.
            str_sql = """ CREATE TABLE `sys_log_datarun` (
            `run_id` text, `snapshot_dt` datetime DEFAULT NULL, `timestamp` datetime DEFAULT NULL) ENGINE = MyISAM;
            """
            pd.io.sql.execute(str_sql, self.conn_fehdw)
            str_sql = """INSERT INTO sys_log_datarun (run_id, snapshot_dt, timestamp) VALUES ('{}', '{}', '{}')
            """.format(run_id, str_snapshot_dt, dt.datetime.now())
            pd.io.sql.execute(str_sql, self.conn_fehdw)

    def has_exceeded_datarun_freq(self, run_id, str_snapshot_dt):
        """ To call before running any data runner, to ensure that running freq does not exceed maximum runs per day.
        :param run_id:
        :param str_snapshot_dt:
        :return:
        """
        # Get number of logged data runs already done (KEY: run_id + snapshot_dt). Note that this has nothing to do with when it was run, or frequency per day!
        str_sql = """
        SELECT * FROM sys_log_datarun
        WHERE run_id = '{}'
        AND snapshot_dt = '{}'
        """.format(run_id, str_snapshot_dt)
        df = pd.read_sql(str_sql, self.conn_fehdw)
        i_log_count = len(df)  # Number of existing log entries.

        # Check the policy table for how many data runs should be allowed.
        str_sql = """
        SELECT * FROM sys_cfg_datarun_freq
        WHERE run_id = '{}'
        """.format(run_id)
        df = pd.read_sql(str_sql, self.conn_fehdw)
        i_cfg_count = int(df['max_freq'][0])

        if i_log_count >= i_cfg_count:  # When it's equal, it means max_freq is hit already.
            return True  # Exceeded max_freq!
        else:
            return False

    def remove_log_datarun(self, run_id, str_snapshot_dt):
        """ Removes one or more data run log entries, for a particular given snapshot_dt of data that was processed.
        :param run_id:
        :param str_snapshot_dt:
        :return:
        """
        if str_snapshot_dt is None:  # Removes ALL log entries for a particular run_id, if str_snapshot_dt is explicitly set to None!
            str_sql = """
            DELETE FROM sys_log_datarun
            WHERE run_id = '{}'
            """.format(run_id)
        else:
            str_sql = """
            DELETE FROM sys_log_datarun
            WHERE run_id = '{}'
            AND snapshot_dt = '{}'
            """.format(run_id, str_snapshot_dt)

        pd.io.sql.execute(str_sql, self.conn_fehdw)

    def drop_and_reload_data_marts(self):
        """ Drops all data marts specified here; reloads them in correct order.
        Due to dependencies, dm1* tables must be re-created before dm2* types.
        Assumption is that the stg* tables are already up-to-date.

        For greater control over the process, something which is dropped will be reloaded soonest possible, instead of dropping all tables at once.
        Each logical group of tables will be treated as a set, and dropped as a set.
        """
        # Create only 1 instance of each class, to avoid having 2 simultaneous instances of the same class, as this causes
        # file logger issues (2nd instance unable to get a lock on the log file and hence cannot write to it).
        of_dr = OccForecastDataRunner()
        op_dr = OperaOTBDataRunner()
        otai_dr = OTAIDataRunner()

        # EzRMS and Market Occ Forecast #
        self.logger.info('DELETING DATA MART: {}'.format('dm1_occ_forecasts_ezrms_mkt'))
        str_sql = """
        DROP TABLE IF EXISTS dm1_occ_forecasts_ezrms_mkt;
        """
        pd.io.sql.execute(str_sql, self.conn_fehdw)
        of_dr.remove_log_datarun(run_id='proc_occ_forecasts', str_snapshot_dt=None)
        of_dr.proc_occ_forecasts_all()

        # Market Occ Forecast #  => Note the dependency on dm1_occ_forecasts_ezrms_mkt!
        self.logger.info('DELETING DATA MART: {}'.format('dm2_occ_forecast_mkt_diff'))
        str_sql = """
        DROP TABLE IF EXISTS dm2_occ_forecast_mkt_diff;
        """
        pd.io.sql.execute(str_sql, self.conn_fehdw)
        of_dr.remove_log_datarun(run_id='proc_occ_forecast_mkt_diff', str_snapshot_dt=None)
        of_dr.proc_occ_forecast_mkt_diff_all()


        # EzRMS Occ Forecast Changes #
        self.logger.info('DELETING DATA MART: {}'.format('dm1_occ_forecast_ezrms, dm2_occ_forecast_ezrms_diff'))
        str_sql = """
        DROP TABLE IF EXISTS dm1_occ_forecast_ezrms, dm2_occ_forecast_ezrms_diff;
        """
        pd.io.sql.execute(str_sql, self.conn_fehdw)
        of_dr.remove_log_datarun(run_id='proc_occ_forecast_ezrms', str_snapshot_dt=None)
        of_dr.remove_log_datarun(run_id='proc_occ_forecast_ezrms_diff', str_snapshot_dt=None)
        of_dr.proc_occ_forecast_ezrms_all()
        of_dr.proc_occ_forecast_ezrms_diff_all()


        # Opera Pickup #
        self.logger.info('DELETING DATA MART: {}'.format('dm1_op_otb_with_allot, dm2_op_otb_with_allot_diff'))
        str_sql = """
        DROP TABLE IF EXISTS dm1_op_otb_with_allot, dm2_op_otb_with_allot_diff;
        """
        pd.io.sql.execute(str_sql, self.conn_fehdw)
        op_dr.remove_log_datarun(run_id='proc_op_otb_with_allot', str_snapshot_dt=None)
        op_dr.remove_log_datarun(run_id='proc_op_otb_with_allot_diff', str_snapshot_dt=None)
        op_dr.proc_op_otb_with_allot_all()
        op_dr.proc_op_otb_with_allot_diff_all()


        # Hotel Price Rank #
        self.logger.info('DELETING DATA MART: {}'.format('dm1_hotel_price_rank'))
        str_sql = """
        DROP TABLE IF EXISTS dm1_hotel_price_rank;
        """
        pd.io.sql.execute(str_sql, self.conn_fehdw)
        otai_dr.remove_log_datarun(run_id='proc_hotel_price_rank', str_snapshot_dt=None)
        otai_dr.proc_hotel_price_rank_all()


class OperaOTBDataRunner(DataRunner):
    """ For generating the Opera OTB with allocation (from CAG file).
    This allows us to have the anticipated number of rm_nts and revenue figures in one place.
    This tells us how many rooms we have booked and the associated revenue (with level of uncertainty determined by the booking_status_code field).
    Writes the result into a data mart for subsequent visualization.
    """
    def __init__(self):
        super().__init__()
        self.APP_NAME = self.config['datarunner']['opera_otb']['app_name']
        self._init_logger(logger_name=self.APP_NAME + '_datareader', app_name=self.APP_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    @dec_err_handler(retries=0)
    def proc_op_otb_with_allot(self, dt_date):
        """ For a given snapshot_dt, generate the Opera OTB, including those entries in the CAG file (Groups and Wholesale).
        From Opera, take the rev+nonrev parts and combine them. Then append the CAG file entries.

        Reads from tables: stg_op_otb_nonrev, stg_op_otb_rev, stg_op_cag.
        Writes to tables: dm1_op_otb_with_allot
        :param dt_date:
        :return:
        """
        run_id = 'proc_op_otb_with_allot'

        str_date_from, str_date_to = feh.utils.split_date(dt_date)

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_date_from):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_date_from))
        else:
            str_sql = """
            SELECT confirmation_no, resort, res_reserv_date, res_reserv_status, res_bkdroom_ct_lbl, sale_comp_name, sale_trav_agent_name, 
            gp_guest_ctry_cde, gp_nationality, gp_origin_code, 'DEF_N' AS 'booking_status_code'
            FROM stg_op_otb_nonrev
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
            """.format(str_date_from, str_date_to)
            df_nonrev = pd.read_sql(str_sql, self.conn_fehdw)

            str_sql = """
            SELECT confirmation_no, resort, business_dt AS stay_date, rev_marketcode, rev_sourcename, rev_proj_room_nts,
            rev_rmrev_extax, rev_food_rev_inctax, rev_oth_rev_inctax
            FROM stg_op_otb_rev
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
            """.format(str_date_from, str_date_to)
            df_rev = pd.read_sql(str_sql, self.conn_fehdw)

            df_merge = df_nonrev.merge(df_rev, how='inner', on=['confirmation_no', 'resort'])
            df_merge.drop(columns=['confirmation_no'], inplace=True)

            str_sql = """
            SELECT resort, allotment_date, label, name, name1, market_code, no_rooms, room_revenue, food_revenue, 
            other_revenue, booking_status_code FROM stg_op_cag
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
            """.format(str_date_from, str_date_to)

            df_cag = pd.read_sql(str_sql, self.conn_fehdw)

            if (len(df_merge) > 0) & (len(df_cag) > 0):
                # The renaming aligns the CAG file column names to become similar to those of Opera's.
                df_cag.rename(columns={'allotment_date': 'stay_date', 'label': 'res_bkdroom_ct_lbl', 'name': 'sale_comp_name',
                                       'name1': 'sale_trav_agent_name', 'market_code': 'rev_marketcode',
                                       'no_rooms': 'rev_proj_room_nts',
                                       'room_revenue': 'rev_rmrev_extax', 'food_revenue': 'rev_food_rev_inctax',
                                       'other_revenue': 'rev_oth_rev_inctax'
                                       }, inplace=True)
                df_cag2 = df_cag[['resort', 'stay_date', 'res_bkdroom_ct_lbl', 'sale_comp_name', 'sale_trav_agent_name',
                                  'rev_marketcode', 'rev_proj_room_nts', 'rev_rmrev_extax', 'rev_food_rev_inctax',
                                  'rev_oth_rev_inctax', 'booking_status_code']]

                df_otb = pd.concat([df_merge, df_cag2], axis=0, ignore_index=True)
                # df_otb.fillna('', inplace=True)  # Removes all NaN and NaT. # Not doing this, because fillna() will cause all data types to change to string!
                df_otb = df_otb[df_merge.columns]  # Re-order the columns. Use df_merge.columns because df_cag.columns are supposed to conform to that of df_merge.
                df_otb.rename(columns={'resort': 'hotel_code'}, inplace=True)  # Standardized on 'hotel_code'.
                df_otb['snapshot_dt'] = pd.to_datetime(str_date_from)

                # WRITE TO DATABASE #
                df_otb.to_sql('dm1_op_otb_with_allot', self.conn_fehdw, index=False, if_exists='append')
                self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_date_from))

                # LOG DATA RUN #
                self.log_datarun(run_id=run_id, str_snapshot_dt=str_date_from)
            else:
                self.logger.error(
                    '[{}] No records found in source tables for snapshot_dt: {}'.format(run_id, str_date_from))

    @dec_err_handler(retries=0)
    def proc_op_otb_with_allot_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        """
        run_id = 'proc_op_otb_with_allot_all'
        l_data_src_tabs = ['stg_op_otb_nonrev', 'stg_op_cag']
        str_func_name = 'proc_op_otb_with_allot'
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to)

    @dec_err_handler(retries=0)
    def proc_op_otb_with_allot_diff(self, dt_date, l_days_diff=[1, 3, 7]):  # We want 1, 3, 7 days_diff.
        """ Dependent on Table: dm1_op_otb_with_allot, and assumes it has been run up-to-date.
        Pre-computes the pick-up in rm_nts and revenues, between 2 snapshot_dts.
        Note on vocab: "df_new" refers to the data set with snapshot_dt as the "reference date".
        "df_old" refers to the data set with snapshot_dt N days ago.

        Reads from table: dm1_op_otb_with_allot
        Writes to table: dm2_op_otb_with_allot_diff

        :param dt_date: The reference date, from which we calculate 1/3/7 days before.
        :param l_days_diff: List of number of days before. Use default values.
        :return: NA
        """
        run_id = 'proc_op_otb_with_allot_diff'

        df_all = DataFrame()

        # Read the records pertaining to str_dt_new outside the for-loop, to avoid unnecessary work.
        str_dt_new = dt.datetime.strftime(dt_date, format='%Y-%m-%d')

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_dt_new):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_dt_new))
        else:
            str_sql = """
            SELECT * FROM dm1_op_otb_with_allot WHERE snapshot_dt = '{}'
            """.format(str_dt_new)
            df_new = pd.read_sql(str_sql, self.conn_fehdw)

            df_new_grp = df_new.groupby(by=['stay_date', 'hotel_code', 'sale_comp_name', 'sale_trav_agent_name',
                                            'res_bkdroom_ct_lbl', 'rev_marketcode', 'booking_status_code'])[['rev_proj_room_nts', 'rev_rmrev_extax', 'rev_food_rev_inctax', 'rev_oth_rev_inctax']].sum()

            for days in l_days_diff:
                str_dt_old = dt.datetime.strftime(dt_date - dt.timedelta(days=days), format='%Y-%m-%d')
                str_sql = """
                SELECT * FROM dm1_op_otb_with_allot WHERE snapshot_dt = '{}'
                """.format(str_dt_old)
                df_old = pd.read_sql(str_sql, self.conn_fehdw)

                # Aggregate (SUM) by the specified dimensions #
                df_old_grp = df_old.groupby(by=['stay_date', 'hotel_code', 'sale_comp_name', 'sale_trav_agent_name',
                                                'res_bkdroom_ct_lbl', 'rev_marketcode', 'booking_status_code'])[['rev_proj_room_nts', 'rev_rmrev_extax', 'rev_food_rev_inctax', 'rev_oth_rev_inctax']].sum()

                if (len(df_new_grp) > 0) & (len(df_old_grp) > 0):  # Ensure that both df have records, to avoid crash when doing subtract().
                    # Get the difference between the values of all columns. Recall that each df represents a data set for each snapshot_dt.
                    df_diff = df_new_grp.subtract(df_old_grp)  # For subtract(), if indices are mismatched, the resultant set will contain the union of both dataframe's index!

                    # Bring in the original 'rev_proj_room_nts' as 'rev_proj_room_nts_new', for occupancy calculation at the Viz level.
                    df_diff = df_diff.merge(df_new_grp[['rev_proj_room_nts', 'rev_rmrev_extax']], how='left',
                                            left_index=True, right_index=True, suffixes=('_diff', '_new'))
                    # Note: We dropna() only AFTER the merge, because we want to eliminate only the df_old_grp indices. This way, some storage space is saved, as the entire row contains only NaNs.
                    # The "rev_proj_room_nts_new" and "rev_rmrev_extax_new" fields come from df_new_grp (ie: data set of the "reference date").
                    df_diff.dropna(axis=0, how='all', inplace=True)

                    # Tidying up. Putting in of new columns.
                    df_diff.reset_index(inplace=True, drop=False)
                    df_diff['days_ago'] = days  # New column to store how many days ago
                    df_diff['snapshot_dt'] = dt_date

                    df_all = df_all.append(df_diff)

            # Bring in the hotel room inventory as another column, for calculating Occupancy #
            str_sql = """ SELECT old_code as hotel_code, room_inventory FROM cfg_map_properties WHERE asset_type = 'hotel' AND operator = 'feh' """
            df_hotel_rms = pd.read_sql(str_sql, self.conn_fehdw)
            df_all = df_all.merge(df_hotel_rms, how='left', on=['hotel_code'])
            df_all = df_all[['snapshot_dt', 'days_ago', 'stay_date', 'hotel_code', 'room_inventory', 'sale_comp_name',
                             'sale_trav_agent_name', 'res_bkdroom_ct_lbl', 'rev_marketcode', 'booking_status_code',
                             'rev_proj_room_nts_new', 'rev_proj_room_nts_diff', 'rev_rmrev_extax_new',
                             'rev_rmrev_extax_diff', 'rev_food_rev_inctax', 'rev_oth_rev_inctax']]
            df_all.rename(columns={'rev_food_rev_inctax': 'rev_food_rev_inctax_diff', 'rev_oth_rev_inctax': 'rev_oth_rev_inctax_diff'}, inplace=True)

            # WRITE TO DATABASE #
            df_all.to_sql('dm2_op_otb_with_allot_diff', self.conn_fehdw, index=False, if_exists='append')
            self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_dt_new))

            # LOG DATA RUN #
            self.log_datarun(run_id=run_id, str_snapshot_dt=str_dt_new)

    @dec_err_handler(retries=0)
    def proc_op_otb_with_allot_diff_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        """
        run_id = 'proc_op_otb_with_allot_diff_all'
        l_data_src_tabs = ['dm1_op_otb_with_allot']
        str_func_name = 'proc_op_otb_with_allot_diff'
        i_dt_from_offset = 1
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to, i_dt_from_offset=i_dt_from_offset)

    def run(self):
        pass


class OccForecastDataRunner(DataRunner):
    """ For generating the Occupancy forecast, based on FWK's predicted demand (rm_nts).
    Loads the model's coefficients, and uses these to compute the predicted Market Occupancy.
    Writes the result into a data mart for subsequent visualization.
    """
    def __init__(self):
        super().__init__()
        self.APP_NAME = self.config['datarunner']['demand_forecast']['app_name']
        self._init_logger(logger_name=self.APP_NAME + '_datareader', app_name=self.APP_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    def _calc_occ_forecast(self, rm_nts=None):
        """ Given the forecasted room nights number, output the occ percentage.
        Uses the pre-computed coefficients from the linear model.
        :param rm_nts:
        :return: A float that represents a percentage, rounded off to the 4th decimal (eg: 0.8123)
        """
        x = float(rm_nts)  # Must be integer.
        x0 = float(self.config['datarunner']['demand_forecast']['coefficients']['x0'])
        x1 = float(self.config['datarunner']['demand_forecast']['coefficients']['x1'])
        x2 = float(self.config['datarunner']['demand_forecast']['coefficients']['x2'])
        x3 = float(self.config['datarunner']['demand_forecast']['coefficients']['x3'])
        x4 = float(self.config['datarunner']['demand_forecast']['coefficients']['x4'])
        x5 = float(self.config['datarunner']['demand_forecast']['coefficients']['x5'])

        # Set a ceiling and floor on the range of possible occ values. Accounting for model's robustness.
        f_max = float(self.config['datarunner']['demand_forecast']['max'])
        f_min = float(self.config['datarunner']['demand_forecast']['min'])

        y = x5 * x**5 + x4 * x**4 + x3 * x**3 + x2 * x**2 + x1 * x**1 + x0
        y = round(y / 100, 4)  # Standardize to a value between 0 to 1, by dividing by 100.

        # Set cap and floor on possible return values #
        if y > f_max:
            y = f_max
        elif y < f_min:
            y = f_min

        return y

    @dec_err_handler(retries=0)
    def proc_occ_forecasts(self, dt_date=dt.datetime.today()):
        """ Generates data mart with BOTH EzRMS forecast and FWK's market forecast for viz use.
        Uses FWK's predicted demand to calculate the Forecasted Mkt Occ Rate.
        Merges the result with EzRMS Occ Forecast. Writes to data mart level 1.
        Does this for only 1 specified date.

        Reads from tables: stg_fwk_proj, stg_ezrms_forecast, cfg_map_properties
        Writes to tables: dm1_occ_forecasts_ezrms_mkt

        :param dt_date:
        :return: NA
        """
        run_id = 'proc_occ_forecasts'

        # GET FWK DATA; CALC MKT OCC FORECAST BASED ON FWK DATA; MERGE WITH EZRMS FORECAST #
        str_date_from, str_date_to = feh.utils.split_date(dt_date)

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_date_from):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_date_from))
        else:
            str_sql_fwk = """
            SELECT snapshot_dt, periodFrom AS stay_date, SUM(value) AS rm_nts FROM stg_fwk_proj
            WHERE classification in ('lengthOfStay1', 'lengthOfStay2', 'lengthOfStay3', 'lengthOfStay4')
            AND snapshot_dt >= '{}' AND snapshot_dt < '{}' 
            GROUP BY snapshot_dt, stay_date
            ORDER BY snapshot_dt, stay_date
            """.format(str_date_from, str_date_to)

            df_fwk = pd.read_sql(str_sql_fwk, self.conn_fehdw)

            str_sql_ezrms = """
            SELECT A.*, B.room_inventory FROM 
            (SELECT snapshot_dt, date AS stay_date, hotel_code, occ_rooms AS rm_nts FROM stg_ezrms_forecast
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}' ) AS A
            INNER JOIN 
            (SELECT old_code AS hotel_code, room_inventory FROM cfg_map_properties
            WHERE asset_type = 'hotel' AND operator = 'feh') AS B            
            ON A.hotel_code = B.hotel_code
            """.format(str_date_from, str_date_to)

            df_ezrms = pd.read_sql(str_sql_ezrms, self.conn_fehdw)

            if (len(df_fwk) > 0) & (len(df_ezrms) > 0):  # Need both df to have at least 1 record, else error when pd.to_datetime() is called.
                df_fwk['snapshot_dt'] = pd.to_datetime(
                    df_fwk['snapshot_dt'].dt.date)  # Keep only the date part, as we wish to JOIN using snapshot_dt.
                df_ezrms['snapshot_dt'] = pd.to_datetime(df_ezrms['snapshot_dt'].dt.date)  # Keep only the date part.

                df_fwk['occ_forecast_mkt'] = df_fwk['rm_nts'].apply(
                    self._calc_occ_forecast)  # Calc the occ forecast based on FWK input.

                # MERGE #
                df_merge = df_ezrms.merge(df_fwk, how='inner', on=['snapshot_dt', 'stay_date'], suffixes=('_ezrms', '_fwk'))
                # WRITE TO DATABASE #
                df_merge.to_sql('dm1_occ_forecasts_ezrms_mkt', self.conn_fehdw, index=False, if_exists='append')
                self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_date_from))

            # LOG DATA RUN #
            self.log_datarun(run_id=run_id, str_snapshot_dt=str_date_from)

    @dec_err_handler(retries=0)
    def proc_occ_forecasts_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        Given a range of dates, to run proc_occ_forecasts() 1 date at a time. Inclusive of both dates.
        Can override either or both dates if desired.
        Set str_dt_from = str_dt_to, if you want to run this 1 day at a time in steady state.
        """
        run_id = 'proc_occ_forecasts_all'
        l_data_src_tabs = ['stg_ezrms_forecast', 'stg_fwk_proj']
        str_func_name = 'proc_occ_forecasts'
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to)

    @dec_err_handler(retries=0)
    def proc_occ_forecast_mkt_diff(self, dt_date, l_days_diff=[3, 7, 14]):  # Comparative intervals controlled here in l_days_diff parameter.
        """ Pre-computes the difference in market (ie: FWK) forecast, between 2 snapshot_dts, for the interval periods given in l_days_diff.
        Reads from dm1_occ_forecasts_ezrms_mkt because the FWK data there is already nicely processed.
        Differs from EzRMS, because for EzRMS, we want to be able to process EzRMS data independently of FWK's data.
        The converse does not apply (ie: no requirement for FWK's data to be processed indepedently of EzRMS data.
        In other words, there is a dependency on EzRMS data being present, otherwise dm1_occ_forecasts_ezrms_mkt will not be populated.

        Reads from tables: dm1_occ_forecasts_ezrms_mkt
        Writes to tables: dm2_occ_forecast_mkt_diff

        :param dt_date:
        :param l_days_diff:
        :return: NA
        """
        run_id = 'proc_occ_forecast_mkt_diff'

        df_all = DataFrame()

        str_date = dt.datetime.strftime(dt_date, format='%Y-%m-%d')

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_date):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_date))
        else:
            str_sql = """
            SELECT DISTINCT snapshot_dt, stay_date, occ_forecast_mkt FROM dm1_occ_forecasts_ezrms_mkt
            WHERE snapshot_dt='{}'
            """.format(str_date)

            df = pd.read_sql(str_sql, self.conn_fehdw)

            for days in l_days_diff:  # Process for each day offset.
                df['offset_col_key'] = df['snapshot_dt'] - dt.timedelta(days=days)  # For joining to the other table

                dt_date_offset = dt_date - dt.timedelta(days=days)
                str_date_offset = dt.datetime.strftime(dt_date_offset, format='%Y-%m-%d')

                str_sql_offset = """
                SELECT DISTINCT snapshot_dt, stay_date, occ_forecast_mkt FROM dm1_occ_forecasts_ezrms_mkt
                WHERE snapshot_dt='{}'
                """.format(str_date_offset)
                df_offset = pd.read_sql(str_sql_offset, self.conn_fehdw)

                df_merge = df.merge(df_offset, how='inner', left_on=['offset_col_key', 'stay_date'],
                                    right_on=['snapshot_dt', 'stay_date'], suffixes=('_new', '_old'))
                df_merge['occ_forecast_mkt_diff'] = df_merge['occ_forecast_mkt_new'] - df_merge['occ_forecast_mkt_old']
                df_merge['occ_forecast_mkt_diff'] = df_merge['occ_forecast_mkt_diff'].round(4)  # Round the percentage to 4 decimals.
                df_merge = df_merge[['snapshot_dt_new', 'stay_date', 'occ_forecast_mkt_new', 'occ_forecast_mkt_old', 'occ_forecast_mkt_diff']]
                df_merge.columns = ['snapshot_dt', 'stay_date', 'occ_forecast_mkt_new', 'occ_forecast_mkt_old', 'occ_forecast_mkt_diff']
                df_merge['days_ago'] = days

                df_all = df_all.append(df_merge, ignore_index=True)
            # WRITE TO DATABASE #
            df_all.to_sql('dm2_occ_forecast_mkt_diff', self.conn_fehdw, index=False, if_exists='append')
            self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_date))

    @dec_err_handler(retries=0)
    def proc_occ_forecast_mkt_diff_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        """
        run_id = 'proc_occ_forecast_mkt_diff_all'
        l_data_src_tabs = ['dm1_occ_forecasts_ezrms_mkt']
        str_func_name = 'proc_occ_forecast_mkt_diff'
        i_dt_from_offset = 3
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to, i_dt_from_offset=i_dt_from_offset)

    @dec_err_handler(retries=0)
    def proc_occ_forecast_ezrms(self, dt_date=dt.datetime.today()):
        """ Processes the EzRMS data into a form that is suitable for visualization, for the 1 given date only.
        This is very similar to proc_occ_forecasts(), but was created so as not to have a dependency on having FWK data,
        ie: EzRMS forecasts can be processed independently, with a data mart of its own.
        :param dt_date:
        :return: NA
        """
        run_id = 'proc_occ_forecast_ezrms'

        # Get ONLY 1 snapshot_dt worth of data. For use on a daily basis.
        str_date_from, str_date_to = split_date(dt_date)

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_date_from):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_date_from))
        else:
            str_sql_ezrms = """
            SELECT A.*, B.room_inventory FROM 
            (SELECT snapshot_dt, date AS stay_date, hotel_code, occ_rooms AS rm_nts_ezrms FROM stg_ezrms_forecast
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}' ) AS A
            INNER JOIN 
            (SELECT old_code AS hotel_code, room_inventory FROM cfg_map_properties
            WHERE asset_type = 'hotel' 
            AND operator = 'feh') AS B            
            ON A.hotel_code = B.hotel_code
            """.format(str_date_from, str_date_to)

            df_ezrms = pd.read_sql(str_sql_ezrms, self.conn_fehdw)

            if len(df_ezrms) > 0:
                df_ezrms['snapshot_dt'] = pd.to_datetime(df_ezrms['snapshot_dt'].dt.date)  # Keep only the date part.
                # WRITE TO DATABSE #
                df_ezrms.to_sql('dm1_occ_forecast_ezrms', self.conn_fehdw, index=False, if_exists='append')
                self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_date_from))
            else:
                self.logger.error('[{}] No records found in source table for snapshot_dt: {}'.format(run_id, str_date_from))

            # LOG DATARUN #
            self.log_datarun(run_id=run_id, str_snapshot_dt=str_date_from)

    @dec_err_handler(retries=0)
    def proc_occ_forecast_ezrms_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        """
        run_id = 'proc_occ_forecast_ezrms_all'
        l_data_src_tabs = ['stg_ezrms_forecast']
        str_func_name = 'proc_occ_forecast_ezrms'
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to)

    @dec_err_handler(retries=0)
    def proc_occ_forecast_ezrms_diff(self, dt_date, l_days_diff=[3, 7, 14]):  # Comparative intervals controlled here in l_days_diff parameter.
        """ Pre-computes the difference in EzRMS forecast, between 2 snapshot_dts, for the interval periods given in l_days_diff.
        :param dt_date:
        :param l_days_diff:
        :return: NA
        """
        run_id = 'proc_occ_forecast_ezrms_diff'

        df_all = DataFrame()
        str_date = dt.datetime.strftime(dt_date, format='%Y-%m-%d')

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_date):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_date))
        else:
            str_sql = """
            SELECT snapshot_dt, stay_date, hotel_code, rm_nts_ezrms, room_inventory FROM dm1_occ_forecast_ezrms
            WHERE snapshot_dt='{}'
            """.format(str_date)

            df = pd.read_sql(str_sql, self.conn_fehdw)

            for days in l_days_diff:  # Process for each day's offset.
                df['offset_col_key'] = df['snapshot_dt'] - dt.timedelta(days=days)  # For joining to the other table.

                dt_date_offset = dt_date - dt.timedelta(days=days)
                str_date_offset = dt.datetime.strftime(dt_date_offset, format='%Y-%m-%d')

                str_sql_offset = """
                SELECT snapshot_dt, stay_date, hotel_code, rm_nts_ezrms, room_inventory FROM dm1_occ_forecast_ezrms
                WHERE snapshot_dt='{}'
                """.format(str_date_offset)
                df_offset = pd.read_sql(str_sql_offset, self.conn_fehdw)

                df_merge = df.merge(df_offset, how='inner',
                                    left_on=['offset_col_key', 'stay_date', 'hotel_code', 'room_inventory'],
                                    right_on=['snapshot_dt', 'stay_date', 'hotel_code', 'room_inventory'])
                df_merge['rm_nts_ezrms_diff'] = df_merge['rm_nts_ezrms_x'] - df_merge['rm_nts_ezrms_y']

                df_merge = df_merge[['snapshot_dt_x', 'stay_date', 'hotel_code', 'rm_nts_ezrms_x', 'rm_nts_ezrms_y', 'rm_nts_ezrms_diff', 'room_inventory']]
                df_merge.columns = ['snapshot_dt', 'stay_date', 'hotel_code', 'rm_nts_ezrms_new', 'rm_nts_ezrms_old', 'rm_nts_ezrms_diff', 'room_inventory']
                df_merge['days_ago'] = days

                df_all = df_all.append(df_merge, ignore_index=True)

            # WRITE TO DATABASE #
            df_all.to_sql('dm2_occ_forecast_ezrms_diff', self.conn_fehdw, index=False, if_exists='append')
            self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_date))

            # LOG DATA RUN #
            self.log_datarun(run_id=run_id, str_snapshot_dt=str_date)

    @dec_err_handler(retries=0)
    def proc_occ_forecast_ezrms_diff_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        """
        run_id = 'proc_occ_forecast_ezrms_diff_all'
        l_data_src_tabs = ['dm1_occ_forecast_ezrms']
        str_func_name = 'proc_occ_forecast_ezrms_diff'
        i_dt_from_offset = 3
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to, i_dt_from_offset=i_dt_from_offset)

    def run(self, dt_date=dt.datetime.today()):
        # In steady state, no need to supply dt_date; can just use default of today().
        # Logically, if_exists='replace' AND dt_date='all' must go together!

        # Ensure both FWK and EzRMS Forecast data have already been loaded for today #
        # has_been_loaded() -> dt_date defaults to today()
        if self.has_been_loaded(source='fwk', dest='mysql', file='FWK_PROJ%%') & \
          self.has_been_loaded(source='ezrms', dest='mysql', file='forecast'):

            self.proc_occ_forecasts(dt_date=dt_date)
        else:
            self.logger.error('Missing load for FWK and/or EzRMS Forecast data for today.')

        # Process EzRMS data marts independently of FWK data #
        if self.has_been_loaded(source='ezrms', dest='mysql', file='forecast'):
            self.proc_occ_forecast_ezrms(dt_date=dt_date)

            if dt_date == 'all':
                self.proc_occ_forecast_ezrms_diff_all()  # Will iterate through all possible dates in dm1_occ_forecast_ezrms.
            else:
                # Just run for this 1 date.
                str_dt_date = dt.datetime.strftime(dt_date, format('%Y-%m-%d'))  # Assume dt_date is a date type
                self.proc_occ_forecast_ezrms_diff_all(str_dt_from=str_dt_date, str_dt_to=str_dt_date)  # dm2. Dependent on proc_occ_forecast_ezrms() running successfully.
        else:
            self.logger.error('Missing load for EzRMS Forecast data for today.')


class OTAIDataRunner(DataRunner):
    """ For generating relative ranking (by price) of all the hotels in the OTAI compset.
    Includes FEH's hotels in the ranking as well.
    Writes the result into a data mart for subsequent visualization.
    """
    def __init__(self):
        super().__init__()
        self.APP_NAME = self.config['datarunner']['hotel_price_rank']['app_name']
        self._init_logger(logger_name=self.APP_NAME + '_datareader', app_name=self.APP_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    @dec_err_handler(retries=0)
    def proc_hotel_price_rank(self, dt_date=dt.datetime.today()):
        """ Ranks the hotels by price, based on OTAI price data, for a given snapshot_dt (the data comes in by snapshots daily).
        Highest price has higher rank (ie: smaller number); close-outs get a value of 1. It's okay to share the same rank. There will also be skipped numbers in "rank".

        Reads from tables: stg_otai_rates, stg_otai_hotels
        Writes to tables: dm1_hotel_price_rank

        :param dt_date:
        :return: NA
        """
        run_id = 'proc_hotel_price_rank'

        str_date_from, str_date_to = feh.utils.split_date(dt_date)

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_date_from):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_date_from))
        else:
            str_sql = """
            SELECT snapshot_dt AS snapshot_dt, ArrivalDate AS stay_date, HotelID AS hotel_id, HotelName AS hotel_name, Value AS price FROM stg_otai_rates
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
            """.format(str_date_from, str_date_to)

            df = pd.read_sql(str_sql, self.conn_fehdw)
            if len(df) > 0:
                df['snapshot_dt'] = pd.DatetimeIndex(df['snapshot_dt']).normalize()  # Removes the time component from the DateTime.

                df.loc[df['price'] == 0, 'price'] = 10000  # Use magic number = "10000", to position this as the highest rank.
                df['rank'] = df.groupby(by=['snapshot_dt', 'stay_date'])['price'].rank(ascending=False, method='min')
                df.loc[df['price'] == 10000, 'price'] = 0  # Locate the magic numbers, and change them back to 0 (their original value).

                # Create 'stay_date_dow' column.
                di_dow = {0: 'MON', 1: 'TUE', 2: 'WED', 3: 'THU', 4: 'FRI', 5: 'SAT', 6: 'SUN'}
                df['stay_date_dow'] = df['stay_date'].dt.dayofweek
                df['stay_date_dow'] = df['stay_date_dow'].apply(lambda x: str(x) + '-' + di_dow[x])  # So that days of week will be sorted in correct order.

                # BRING IN COMP SET PRICING FOR EACH ROW IN DF #
                str_sql = "SELECT HotelID AS hotel_id, CompetitorID AS comp_hotel_id, CompetitorName AS comp_hotel_name FROM stg_otai_hotels"
                df_compset = pd.read_sql(str_sql, self.conn_fehdw)

                # Now each snapshot_dt/stay_date/hotel_id will be multiplied by the number of rows in the compset for that hotel, and gain the comp_hotel_id column.
                df_merge = df.merge(df_compset, how='left', on=['hotel_id'])
                df_copy = df[['stay_date', 'hotel_id', 'price']]  # Can do this because df contains the prices for ALL hotels, including compset hotels!
                df_copy.columns = ['stay_date', 'comp_hotel_id', 'comp_price']  # Rename columns so that easier to merge later.
                df_merge2 = df_merge.merge(df_copy, how='left', on=['stay_date', 'comp_hotel_id'])

                # Bring in the "old hotel code" column. For merging use later in the viz level.
                str_sql = """
                SELECT otai_hotel_id AS hotel_id, old_code AS hotel_code FROM cfg_map_properties
                WHERE operator = 'feh' AND asset_type = 'hotel'
                """
                df_hotel_codes = pd.read_sql(str_sql, self.conn_fehdw)
                df_merge3 = df_merge2.merge(df_hotel_codes, how='left', on=['hotel_id'])
                df_merge3 = df_merge3[['snapshot_dt', 'stay_date', 'hotel_id', 'hotel_code', 'hotel_name', 'price',
                                       'rank', 'stay_date_dow', 'comp_hotel_id', 'comp_hotel_name', 'comp_price']]
                # WRITE TO DATABASE #
                df_merge3.to_sql('dm1_hotel_price_rank', self.conn_fehdw, index=False, if_exists='append')
                self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_date_from))

                # LOG DATA RUN #
                self.log_datarun(run_id=run_id, str_snapshot_dt=str_date_from)
            else:
                self.logger.error('[{}] No records found in source table for snapshot_dt: {}'.format(run_id, str_date_from))

    @dec_err_handler(retries=0)
    def proc_hotel_price_rank_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        """
        run_id = 'proc_hotel_price_rank_all'
        l_data_src_tabs = ['stg_otai_rates']
        str_func_name = 'proc_hotel_price_rank'
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to)

    @dec_err_handler(retries=0)
    def proc_hotel_price_otb_evolution(self, dt_date=dt.datetime.today()):
        """ Creates the data mart which supports the viz showing how 1) FEH Hotel Price, and 2) OTB Occ, evolves over snapshot_dts.

        Reads from tables: stg_otai_rates, dm1_op_otb_with_allot, cfg_map_properties
        Writes to tables: dm2_hotel_price_otb_evolution

        :param dt_date:
        :return: NA
        """
        run_id = 'proc_hotel_price_otb_evolution'
        str_date_from, str_date_to = feh.utils.split_date(dt_date)

        if self.has_exceeded_datarun_freq(run_id=run_id, str_snapshot_dt=str_date_from):
            self.logger.info('[{}] SKIPPING. Data already processed for snapshot_dt: {}'.format(run_id, str_date_from))
        else:
            # Get OTB #
            str_sql_otb = """
            SELECT A.*, B.room_inventory, B.otai_hotel_id FROM 
            (SELECT snapshot_dt, stay_date, hotel_code, booking_status_code, rev_marketcode, rev_proj_room_nts FROM dm1_op_otb_with_allot) AS A
            INNER JOIN
            (SELECT old_code AS hotel_code, otai_hotel_id, hotel_name, room_inventory FROM cfg_map_properties
            WHERE operator = 'feh' AND asset_type = 'hotel') AS B
            ON A.hotel_code = B.hotel_code
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
            """.format(str_date_from, str_date_to)

            df_otb = pd.read_sql(str_sql_otb, self.conn_fehdw)

            # Get OTAI Prices #
            str_sql_otai = """
            SELECT HotelID AS otai_hotel_id, ArrivalDate AS stay_date, Value AS price FROM stg_otai_rates
            WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
            """.format(str_date_from, str_date_to)

            df_otai = pd.read_sql(str_sql_otai, self.conn_fehdw)

            if (len(df_otb) > 0) & (len(df_otai) > 0):
                df_merge = df_otb.merge(df_otai, how='inner', on=['otai_hotel_id', 'stay_date'])

                # WRITE TO DATABASE #
                df_merge.to_sql('dm2_hotel_price_otb_evolution', self.conn_fehdw, index=False, if_exists='append')
                self.logger.info('[{}] Data processed for snapshot_dt: {}'.format(run_id, str_date_from))

                # LOG DATA RUN #
                self.log_datarun(run_id=run_id, str_snapshot_dt=str_date_from)
            else:
                self.logger.error('[{}] No records found in source tables for snapshot_dt: {}'.format(run_id, str_date_from))

    @dec_err_handler(retries=0)
    def proc_hotel_price_otb_evolution_all(self, str_dt_from=None, str_dt_to=None):
        """ Iterator method for repeated method calling.
        Note: For this case, we still want tables ['stg_otai_rates', 'dm1_op_otb_with_allot'] to have their snapshot_dts
        to be in alignment, so that you can see the Price and Occ OTB for the same snapshot_dts.
        Hence it is okay to use _generic_run_all() to process them (it looks for intersection of snapshot_dt ranges.
        """
        run_id = 'proc_hotel_price_otb_evolution_all'
        l_data_src_tabs = ['stg_otai_rates', 'dm1_op_otb_with_allot']
        str_func_name = 'proc_hotel_price_otb_evolution'
        self._generic_run_all(run_id=run_id, l_data_src_tabs=l_data_src_tabs, str_func_name=str_func_name, str_dt_from=str_dt_from, str_dt_to=str_dt_to)

    def run(self, dt_date=dt.datetime.today()):
        # In steady state, no need to supply dt_date; can just use default of today().
        # has_been_loaded() -> dt_date defaults to today()

        str_date = dt.datetime.strftime(dt_date, format='%Y-%m-%d')

        if self.has_been_loaded(source='otai', dest='mysql', file='rates'):
            self.proc_hotel_price_rank(dt_date=dt_date)  # Do data run for current date.
        else:
            self.logger.error('Missing data load for {} data for snapshot_dt: {}.'.format('OTAI Rates', str_date))