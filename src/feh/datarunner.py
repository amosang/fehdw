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

    def has_been_loaded(self, source, dest, file, dt_date = dt.datetime.today()):
        """ Check if a data source has been loaded for a given date.
        :param source: data source system (eg: 'opera').
        :param dest: data dest system (eg: 'mysql').
        :param file: Filename. To log each filename (don't include the path).
        :param dt_date: Date to search for. Defaults to current date, if not provided.
        :return:
        """
        str_date_from, str_date_to = split_date(dt_date)

        # Get number of logged data loads already done within the same day (KEY: source/dest/file).
        str_sql = """
        SELECT * FROM sys_log_dataload
        WHERE source = '{}'
        AND dest = '{}'
        AND file LIKE '{}'
        AND timestamp >= '{}' AND timestamp < '{}' 
        """.format(source, dest, file, str_date_from, str_date_to)
        df = pd.read_sql(str_sql, self.conn_fehdw)
        i_log_count = len(df)  # Number of existing log entries.
        if i_log_count > 0:
            return True
        else:
            return False


class DemandForecastDataRunner(DataRunner):
    def __init__(self):
        super().__init__()
        self.APP_NAME = self.config['datarunner']['demand_forecast']['app_name']
        self._init_logger(logger_name=self.APP_NAME + '_datareader', app_name=self.APP_NAME)

    def __del__(self):
        super().__del__()
        self._free_logger()

    def _get_fwk_proj(self, dt_date=None):
        """ Get FWK's projected demand for per snapshot per stay_date. We loaded this earlier.
        :param dt_date: If provided, will get snapshot_dt = dt_date only.
        :return: DataFrame
        """
        if dt_date is None:
            # Read whole table. One-time use only.
            str_sql = """
            SELECT snapshot_dt, periodFrom AS stay_date, SUM(value) AS count FROM stg_fwk_proj
            WHERE classification in ('lengthOfStay1', 'lengthOfStay2', 'lengthOfStay3', 'lengthOfStay4')
            GROUP BY snapshot_dt, stay_date
            ORDER BY snapshot_dt, stay_date
            """
        else:
            # Get only 1 snapshot_dt worth of data.
            str_date_from, str_date_to = split_date(dt_date)
            str_sql = """
            SELECT snapshot_dt, periodFrom AS stay_date, SUM(value) AS count FROM stg_fwk_proj
            WHERE classification in ('lengthOfStay1', 'lengthOfStay2', 'lengthOfStay3', 'lengthOfStay4')
            AND snapshot_dt >= '{}' AND snapshot_dt < '{}' 
            GROUP BY snapshot_dt, stay_date
            ORDER BY snapshot_dt, stay_date
            """.format(str_date_from, str_date_to)

        df = pd.read_sql(str_sql, self.conn_fehdw)
        return df

    def _calc_demand_forecast(self, df_fwk):
        """ Given the FWK's market forecast, calculate the market demand (UoM: rm_nts).
        :param df_fwk:
        :return:
        """
        pass

    def _calc_occ_forecast(self, rm_nts=None):
        """ Given the forecasted room nights, output the percentage
        :param rm_nts:
        :return: A float, representing the percentage, rounded off to the 4th decimal (eg: 0.8123)
        """
        x = float(rm_nts)  # Must be integer.
        x0 = float(self.config['datarunner']['demand_forecast']['coefficients']['x0'])
        x1 = float(self.config['datarunner']['demand_forecast']['coefficients']['x1'])
        x2 = float(self.config['datarunner']['demand_forecast']['coefficients']['x2'])
        x3 = float(self.config['datarunner']['demand_forecast']['coefficients']['x3'])
        x4 = float(self.config['datarunner']['demand_forecast']['coefficients']['x4'])
        x5 = float(self.config['datarunner']['demand_forecast']['coefficients']['x5'])

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

    def run(self):
        # Ensure both FWK and EzRMS Forecast data have already been loaded for today #
        # has_been_loaded() -> dt_date defaults to today()
        if self.has_been_loaded(source='fwk', dest='mysql', file='FWK_PROJ%%') & \
        self.has_been_loaded(source='ezrms', dest='mysql', file='forecast'):
            # Get FWK PROJECTED DEMAND #
            df_fwk = self._get_fwk_proj()
            df_demand_fc = self._calc_demand_forecast(df_fwk)

            # Get EzRMS
        else:
            self.logger.error('Missing load for FWK and/or EzRMS Forecast data for today.')





