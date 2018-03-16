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
        #self._init_logger(logger_name='datarunner', app_name=self.APP_NAME)

    def __del__(self):
        self.conn_fehdw.close()
        #self._free_logger()

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
        str_format = '[%(asctime)s]-[%(levelname)s]- %(message)s'
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

    def has_been_run(self, run_id, str_snapshot_dt, dt_date = dt.datetime.today()):
        """ Check if a data source has been run for a given date.
        :param run_id:
        :param str_snapshot_dt:
        :param dt_date:
        :return:
        """
        str_date_from, str_date_to = split_date(dt_date)

        # Get number of logged data runs already run within the same day (KEY: run_id).
        str_sql = """
        SELECT * FROM sys_log_datarun
        WHERE run_id = '{}'
        AND snapshot_dt = '{}'
        AND timestamp >= '{}' AND timestamp < '{}' 
        """.format(run_id, str_snapshot_dt, str_date_from, str_date_to)
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
        # Get number of logged data runs already done within the same day (KEY: run_id).
        str_sql = """
        SELECT * FROM sys_log_datarun
        WHERE run_id = '{}'
        AND snapshot_dt = '{}'
        AND timestamp >= '{}' AND timestamp < '{}' 
        """.format(run_id, str_snapshot_dt, dt.datetime.now().date(), dt.datetime.now().date() + dt.timedelta(days=1))
        df = pd.read_sql(str_sql, self.conn_fehdw)
        i_log_count = len(df)  # Number of existing log entries.

        # Check the policy table for how many data runs should be allowed.
        str_sql = """
        SELECT * FROM sys_cfg_datarun_freq
        WHERE run_id = '{}'
        AND snapshot_dt = '{}'
        """.format(run_id, str_snapshot_dt)
        df = pd.read_sql(str_sql, self.conn_fehdw)
        i_cfg_count = int(df['max_freq'][0])

        if i_log_count >= i_cfg_count:  # When it's equal, it means max_freq is hit already.
            return True  # Exceeded max_freq!
        else:
            return False

    def remove_log_datarun(self, run_id, str_snapshot_dt, str_date):
        """ Removes one or more data run log entries, for a particular given date.
        :param run_id:
        :param str_snapshot_dt:
        :param str_date:
        :return:
        """
        str_sql = """
        DELETE FROM sys_log_datarun
        WHERE run_id = '{}'
        AND snapshot_dt = '{}'
        AND DATE(timestamp) = '{}'
        """.format(run_id, str_snapshot_dt, str_date)

        pd.io.sql.execute(str_sql, self.conn_fehdw)


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

    def proc_opera_otb_with_allot(self, dt_date):
        str_date_from, str_date_to = split_date(dt_date)

        str_sql = f"""
        SELECT A.*, B.business_dt, B.rev_marketcode, B.rev_ratecode, B.rev_sourcecode, B.rev_sourcename, B.rev_eff_rate_amt, B.rev_proj_room_nts,
        B.rev_rmrev_extax, B.rev_food_rev_inctax, B.rev_oth_rev_inctax, B.rev_hurdle, B.rev_hurdle_override, B.rev_restriction_override
        FROM
            (SELECT confirmation_no, resort, res_reserv_date, res_reserv_status, res_bkdroom_ct_lbl, res_roomty_lbl, res_cancel_date, res_pay_method, res_credcard_ty,
            allot_allot_book_cde, allot_allot_book_desc, arr_arrival_dt, arr_arrival_time, arr_departure_dt, arr_departure_time,
            sale_comp_name, sale_zip_code, sale_sales_rep, sale_sales_rep_code, sale_industry_code, sale_trav_agent_name, sale_sales_rep1, sale_sales_rep_code1,
            sale_all_owner, gp_guest_ctry_cde, gp_nationality, gp_origin_code, gp_guest_firstname, gp_guest_lastname, gp_guest_title_code, gp_spcl_req_desc, gp_email,
            gp_dob, gp_vip_status_desc, ng_adults, ng_children, ng_extra_beds, ng_cribs, 'DEF_N' AS 'booking_status_code'
            FROM stg_op_otb_nonrev
            WHERE snapshot_dt >= '{str_date_from}' AND snapshot_dt < '{str_date_to}') AS A
                INNER JOIN
            (SELECT confirmation_no, resort, business_dt, rev_marketcode, rev_ratecode, rev_sourcecode, rev_sourcename, rev_eff_rate_amt, rev_proj_room_nts,
            rev_rmrev_extax, rev_food_rev_inctax, rev_oth_rev_inctax, rev_hurdle, rev_hurdle_override, rev_restriction_override
            FROM stg_op_otb_rev
            WHERE snapshot_dt >= '{str_date_from}' AND snapshot_dt < '{str_date_to}' ) AS B
                ON A.confirmation_no = B.confirmation_no AND A.resort = B.resort
        """
        df_otb = pd.read_sql(str_sql, self.conn_fehdw)


    def run(self):
        pass


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

    def proc_hotel_price_rank_all(self, str_dt_from=None, str_dt_to=None):
        pass

    def proc_hotel_price_rank(self, dt_date=dt.datetime.today()):
        # Get ONLY 1 snapshot_dt worth of data. For use on a daily basis.
        str_date_from, str_date_to = split_date(dt_date)

        str_sql = """
        SELECT DATE(snapshot_dt) AS snapshot_dt, ArrivalDate AS stay_date, HotelID AS hotel_id, HotelName AS hotel_name, Value AS price FROM stg_otai_rates
        WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'       
        """.format(str_date_from, str_date_to)

        df = pd.read_sql(str_sql, self.conn_fehdw)
        # CONTINUE HERE #

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

    def proc_ezrms_forecast(self, dt_date=dt.datetime.today()):
        """ Processes the EzRMS data into a form that is suitable for visualization, for the 1 given date only.
        This is very similar to proc_occ_forecasts(), but was created so as not to have a dependency on having FWK data,
        ie: EzRMS forecasts can be processed independently, with a data mart of its own.
        :param dt_date:
        :return: DataFrame
        """
        # Get ONLY 1 snapshot_dt worth of data. For use on a daily basis.
        str_date_from, str_date_to = split_date(dt_date)

        str_sql_ezrms = """
        SELECT A.*, B.room_inventory FROM 
        (SELECT snapshot_dt, date AS stay_date, hotel_code, occ_rooms AS rm_nts_ezrms FROM stg_ezrms_forecast
        WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}' ) AS A
        INNER JOIN 
        (SELECT old_code AS hotel_code, room_inventory FROM cfg_map_hotel_sr
        WHERE hotel_or_sr = 'Hotel') AS B            
        ON A.hotel_code = B.hotel_code
        """.format(str_date_from, str_date_to)

        df_ezrms = pd.read_sql(str_sql_ezrms, self.conn_fehdw)

        if len(df_ezrms) > 0:
            df_ezrms['snapshot_dt'] = pd.to_datetime(df_ezrms['snapshot_dt'].dt.date)  # Keep only the date part.
            df_ezrms.to_sql('dm1_occ_forecast_ezrms', self.conn_fehdw, index=False, if_exists='append')
            self.logger.info('[proc_ezrms_forecast] Completed successfully for Period: {}'.format(str_date_from))

        # LOG DATARUN #
        self.logger.info('Logged data run activity to system log table.')
        self.log_datarun(run_id='proc_ezrms_forecast', str_snapshot_dt=str_date_from)

    def proc_ezrms_forecast_all(self, str_dt_from=None, str_dt_to=None):
        """ Given a range of dates, to run proc_ezrms_forecast() 1 date at a time. Inclusive of both dates.
        Can override either or both dates if desired.
        Set str_dt_from = str_dt_to, if you want to run this 1 day at a time in steady state.
        :param str_dt_from:
        :param str_dt_to:
        :return:
        """
        # Setting upper and lower rational bounds on dt_from and dt_to, to avoid unnecessary processing #
        str_sql = """
        SELECT DATE(MIN(snapshot_dt)) AS min, DATE(MAX(snapshot_dt)) AS max FROM stg_ezrms_forecast
        """
        dt_from = pd.read_sql(str_sql, self.conn_fehdw)['min'][0]
        dt_to = pd.read_sql(str_sql, self.conn_fehdw)['max'][0]

        # Standardize data types to datetime class, instead of datetime.date class, so that subsequent handling is consistent.
        dt_from = pd.to_datetime(dt_from)
        dt_to = pd.to_datetime(dt_to)

        # Replace the bounds, only if so desired. Can selectively choose to overwrite only dt_from OR dt_to.
        if str_dt_from is not None:
            dt_from = pd.to_datetime(str_dt_from)
        if str_dt_to is not None:
            dt_to = pd.to_datetime(str_dt_to)

        for d in range(int((dt_to - dt_from).days) + 1):  # +1 to make it inclusive of the dt_to.
            self.proc_occ_forecasts(dt_date=dt_from+dt.timedelta(days=d))
        self.logger.info('[proc_ezrms_forecasts_all] Completed successfully for period: {} to {}'.format(str(dt_from.date()), str(dt_to.date())))

    def proc_occ_forecasts(self, dt_date=dt.datetime.today()):
        """ Use FWK's predicted demand to calculate the Forecasted Mkt Occ Rate.
        Merges the result with EzRMS Occ Forecast. Writes to data mart level 1.
        Does this for only 1 specified date.
        :param dt_date:
        :return: DataFrame
        """
        # GET FWK DATA; CALC MKT OCC FORECAST BASED ON FWK DATA; MERGE WITH EZRMS FORECAST #
        str_date_from, str_date_to = split_date(dt_date)

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
        (SELECT old_code AS hotel_code, room_inventory FROM cfg_map_hotel_sr
        WHERE hotel_or_sr = 'Hotel') AS B            
        ON A.hotel_code = B.hotel_code
        """.format(str_date_from, str_date_to)

        df_ezrms = pd.read_sql(str_sql_ezrms, self.conn_fehdw)

        if (len(df_fwk) > 0) & (len(df_ezrms) > 0):  # Need both df to have at least 1 record, else error when pd.to_datetime() is called.
            df_fwk['snapshot_dt'] = pd.to_datetime(df_fwk['snapshot_dt'].dt.date)  # Keep only the date part, as we wish to JOIN using snapshot_dt.
            df_ezrms['snapshot_dt'] = pd.to_datetime(df_ezrms['snapshot_dt'].dt.date)  # Keep only the date part.

            df_fwk['occ_forecast_mkt'] = df_fwk['rm_nts'].apply(self._calc_occ_forecast)  # Calc the occ forecast based on FWK input.

            # MERGE #
            df_merge = df_ezrms.merge(df_fwk, how='inner', on=['snapshot_dt', 'stay_date'], suffixes=('_ezrms', '_fwk'))
            df_merge.to_sql('dm1_occ_forecasts_ezrms_mkt', self.conn_fehdw, index=False, if_exists='append')
            self.logger.info('[proc_occ_forecasts] Completed successfully for snapshot_dt: {}'.format(str_date_from))

        # LOG DATARUN #
        self.logger.info('Logged data run activity to system log table.')
        self.log_datarun(run_id='proc_occ_forecasts', str_snapshot_dt=str_date_from)

    def proc_occ_forecasts_all(self, str_dt_from=None, str_dt_to=None):
        """ Given a range of dates, to run proc_occ_forecasts() 1 date at a time. Inclusive of both dates.
        Can override either or both dates if desired.
        Set str_dt_from = str_dt_to, if you want to run this 1 day at a time in steady state.
        """
        # Setting upper and lower rational bounds on dt_from and dt_to, to avoid unnecessary processing #
        str_sql = """
        SELECT DATE(MIN(snapshot_dt)) AS min, DATE(MAX(snapshot_dt)) AS max FROM stg_ezrms_forecast
        """
        dt_from_ezrms = pd.read_sql(str_sql, self.conn_fehdw)['min'][0]
        dt_to_ezrms = pd.read_sql(str_sql, self.conn_fehdw)['max'][0]

        str_sql = """
        SELECT DATE(MIN(snapshot_dt)) AS min, DATE(MAX(snapshot_dt)) AS max FROM stg_fwk_proj
        """
        dt_from_fwk = pd.read_sql(str_sql, self.conn_fehdw)['min'][0]
        dt_to_fwk = pd.read_sql(str_sql, self.conn_fehdw)['max'][0]

        # Determine INTERSECTION of from and to dates, for both ezrms and fwk.
        if dt_from_ezrms < dt_from_fwk:
            dt_from = dt_from_fwk
        else:
            dt_from = dt_from_ezrms

        if dt_to_ezrms < dt_to_fwk:
            dt_to = dt_to_ezrms
        else:
            dt_to = dt_to_fwk

        # Standardize data types to datetime class, instead of datetime.date class, so that subsequent handling is consistent.
        dt_from = pd.to_datetime(dt_from)
        dt_to = pd.to_datetime(dt_to)

        # Replace the bounds, only if so desired. Can selectively choose to overwrite only dt_from OR dt_to.
        if str_dt_from is not None:
            dt_from = pd.to_datetime(str_dt_from)
        if str_dt_to is not None:
            dt_to = pd.to_datetime(str_dt_to)

        for d in range(int((dt_to - dt_from).days) + 1):  # +1 to make it inclusive of the dt_to.
            self.proc_occ_forecasts(dt_date=dt_from+dt.timedelta(days=d))

        self.logger.info('[proc_occ_forecasts_all] Completed successfully for period: {} to {}'.format(str(dt_from.date()), str(dt_to.date())))

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

    def proc_rm_nts_ezrms_diff_all(self, str_dt_from=None, str_dt_to=None):
        """ Given a range of dates, to run proc_rm_nts_ezrms_diff() 1 date at a time. Inclusive of both dates.
        Can override either or both dates if desired.
        Set str_dt_from = str_dt_to, if you want to run this 1 day at a time in steady state.
        """
        # Setting upper and lower rational bounds on dt_from and dt_to, to avoid unnecessary processing.
        str_sql = """
        SELECT MIN(snapshot_dt) AS snapshot_dt_min, MAX(snapshot_dt) AS snapshot_dt_max FROM dm1_occ_forecast_ezrms
        """
        dt_from = pd.read_sql(str_sql, self.conn_fehdw)['snapshot_dt_min'][0] + dt.timedelta(days=3)  # Shift by 3 days, because we want a look-back of at least 3 days.
        dt_to = pd.read_sql(str_sql, self.conn_fehdw)['snapshot_dt_max'][0]

        # Replace the bounds, only if so desired. Can selectively choose to replace only dt_from OR dt_to.
        if str_dt_from is not None:
            dt_from = pd.to_datetime(str_dt_from)
        if str_dt_to is not None:
            dt_to = pd.to_datetime(str_dt_to)

        for d in range(int((dt_to - dt_from).days) + 1):  # +1 to make it inclusive of the dt_to.
            self.proc_rm_nts_ezrms_diff(if_exists='append', dt_date=dt_from+dt.timedelta(days=d))

        if (str_dt_from is None) & (str_dt_to is None):  # handling the 'all' case, where both will be None.
            self.logger.info('[proc_rm_nts_ezrms_diff_all] Completed successfully for period: {} to {}'.format(str(dt_from.date()), str(dt_to.date())))
        else:
            self.logger.info('[proc_rm_nts_ezrms_diff_all] Completed successfully for period: {} to {}'.format(str_dt_from, str_dt_to))

    def proc_rm_nts_ezrms_diff(self, if_exists, dt_date, l_days=[3,7,14]):  # Comparative intervals controlled here in l_days parameter.
        df_all = DataFrame()

        str_date = dt.datetime.strftime(dt_date, format='%Y-%m-%d')
        str_sql = """
        SELECT snapshot_dt, stay_date, hotel_code, rm_nts_ezrms, room_inventory FROM dm1_occ_forecast_ezrms
        WHERE snapshot_dt='{}'
        """.format(str_date)

        df = pd.read_sql(str_sql, self.conn_fehdw)

        for days in l_days:  # Process for each day offset.
            df['offset_col_key'] = df['snapshot_dt'] - dt.timedelta(days=days)  # For joining to the other table

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
        # Write to database.
        df_all.to_sql('dm2_rm_nts_ezrms_diff', self.conn_fehdw, index=False, if_exists=if_exists)

    def proc_occ_fc_mkt_diff_all(self, str_dt_from=None, str_dt_to=None):
        """ Given a range of dates, to run proc_occ_fc_mkt_diff() 1 date at a time. Inclusive of both dates.
        Can override either or both dates if desired.
        Set str_dt_from = str_dt_to, if you want to run this 1 day at a time in steady state.
        """
        # Setting upper and lower rational bounds on dt_from and dt_to, to avoid unnecessary processing.
        str_sql = """
        SELECT MIN(snapshot_dt) AS snapshot_dt_min, MAX(snapshot_dt) AS snapshot_dt_max FROM dm1_occ_forecasts_ezrms_mkt
        """
        dt_from = pd.read_sql(str_sql, self.conn_fehdw)['snapshot_dt_min'][0] + dt.timedelta(days=3)  # Shift by 3 days, because we want a look-back of at least 3 days.
        dt_to = pd.read_sql(str_sql, self.conn_fehdw)['snapshot_dt_max'][0]

        # Replace the bounds, only if so desired. Can selectively choose to replace only dt_from OR dt_to.
        if str_dt_from is not None:
            dt_from = pd.to_datetime(str_dt_from)
        if str_dt_to is not None:
            dt_to = pd.to_datetime(str_dt_to)

        for d in range(int((dt_to - dt_from).days) + 1):  # +1 to make it inclusive of the dt_to.
            self.proc_occ_fc_mkt_diff(if_exists='append', dt_date=dt_from+dt.timedelta(days=d))

        if (str_dt_from is None) & (str_dt_to is None):  # handling the 'all' case, where both will be None.
            self.logger.info('[proc_occ_fc_mkt_diff_all] Completed successfully for period: {} to {}'.format(str(dt_from.date()), str(dt_to.date())))
        else:
            self.logger.info('[proc_occ_fc_mkt_diff_all] Completed successfully for period: {} to {}'.format(str_dt_from, str_dt_to))

    def proc_occ_fc_mkt_diff(self, if_exists, dt_date, l_days=[3,7,14]):  # Comparative intervals controlled here in l_days parameter.
        df_all = DataFrame()

        str_date = dt.datetime.strftime(dt_date, format='%Y-%m-%d')

        str_sql = """
        SELECT DISTINCT snapshot_dt, stay_date, occ_forecast_mkt FROM dm1_occ_forecasts_ezrms_mkt
        WHERE snapshot_dt='{}'
        """.format(str_date)

        df = pd.read_sql(str_sql, self.conn_fehdw)

        for days in l_days:  # Process for each day offset.
            df['offset_col_key'] = df['snapshot_dt'] - dt.timedelta(days=days)  # For joining to the other table

            dt_date_offset = dt_date - dt.timedelta(days=days)
            str_date_offset = dt.datetime.strftime(dt_date_offset, format='%Y-%m-%d')

            str_sql_offset = """
            SELECT DISTINCT snapshot_dt, stay_date, occ_forecast_mkt FROM dm1_occ_forecasts_ezrms_mkt
            WHERE snapshot_dt='{}'
            """.format(str_date_offset)
            df_offset = pd.read_sql(str_sql_offset, self.conn_fehdw)

            df_merge = df.merge(df_offset, how='inner',
                                left_on=['offset_col_key', 'stay_date'], right_on=['snapshot_dt', 'stay_date'])
            df_merge['occ_forecast_mkt_diff'] = df_merge['occ_forecast_mkt_x'] - df_merge['occ_forecast_mkt_y']
            df_merge['occ_forecast_mkt_diff'] = df_merge['occ_forecast_mkt_diff'].round(4)  # Round the percentage to 4 decimals.
            df_merge = df_merge[['snapshot_dt_x', 'stay_date', 'occ_forecast_mkt_diff']]
            df_merge.columns = ['snapshot_dt', 'stay_date', 'occ_forecast_mkt_diff_percent']
            df_merge['days_ago'] = days

            df_all = df_all.append(df_merge, ignore_index=True)
        # Write to database.
        df_all.to_sql('dm2_occ_fc_mkt_diff', self.conn_fehdw, index=False, if_exists=if_exists)

    def run(self, if_exists='replace', dt_date=dt.datetime.today()):  # TODO: Change if_exists to 'append' when stable.
        # In steady state, no need to supply dt_date; can just use default of today().
        # Logically, if_exists='replace' AND dt_date='all' must go together!

        # Ensure both FWK and EzRMS Forecast data have already been loaded for today #
        # has_been_loaded() -> dt_date defaults to today()
        if self.has_been_loaded(source='fwk', dest='mysql', file='FWK_PROJ%%') & \
          self.has_been_loaded(source='ezrms', dest='mysql', file='forecast'):

            self.proc_occ_forecasts(if_exists=if_exists, dt_date=dt_date)
        else:
            self.logger.error('Missing load for FWK and/or EzRMS Forecast data for today.')

        # Process EzRMS data marts independently of FWK data #
        if self.has_been_loaded(source='ezrms', dest='mysql', file='forecast'):
            self.proc_ezrms_forecast(if_exists=if_exists, dt_date=dt_date)

            if dt_date == 'all':
                self.proc_rm_nts_ezrms_diff_all()  # Will iterate through all possible dates in dm1_occ_forecast_ezrms.
            else:
                # Just run for this 1 date.
                str_dt_date = dt.datetime.strftime(dt_date, format('%Y-%m-%d'))  # Assume dt_date is a date type
                self.proc_rm_nts_ezrms_diff_all(str_dt_from=str_dt_date, str_dt_to=str_dt_date)  # dm2. Dependent on proc_ezrms_forecast() running successfully.
        else:
            self.logger.error('Missing load for EzRMS Forecast data for today.')

