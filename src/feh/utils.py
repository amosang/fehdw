import pandas as pd
import sys
import logging
import os
import re
import datetime as dt
import time
import zipfile
from pandas import DataFrame

# Important! We use ReportBot to send admin error emails. As ReportBot resides in another project, we add
# that project's path to sys.path, in order to make use of those functionalities there.
# PyCharm will indicate that the path cannot be resolved, but it still works.
sys.path.insert(0, 'C:/webapps')  # Insert parent dir into path.
from report_bot.report_bot import AdminReportBot


class MaxDataLoadException(Exception):
    pass


def check_dataload_not_logged(t_timenow, conn):
    """ This is called from scheduler_load_data.py. Given a Time, check the scheduled data loads table to see
    which jobs are supposed to have run, then look up the data load logs table to look for corresponding entries.
    If corresponding entries do not exist, this means that the load must have failed.
    :param t_timenow:
    :param conn:
    :return:
    """
    # Return df of "sched" rows where log entry not found for today.
    df_out = DataFrame()
    str_date_from, str_date_to = split_date()

    str_sql = """
    SELECT * FROM sys_cfg_dataload_sched
    """
    df_sched = pd.read_sql(str_sql, conn)

    if len(df_sched) > 0:  # Iterate thru df_sched to see which source/dest/file rows fall within time_from-time_to range.
        for idx, row in df_sched.iterrows():
            time_from = row['time_from']
            time_to = row['time_to']
            t_from = dt.time(int(time_from[:2]), int(time_from[2:]))
            t_to = dt.time(int(time_to[:2]), int(time_to[2:]))
            # For each scheduled task that is supposed to have run, check if log entry exists, indicating successful run #
            # There will not be repeated alert emails sent, because the monitoring happens immediately after the job which generates the logs, both of which run in the same half hour time window!
            if t_from <= t_timenow < t_to:
                # CHECK IF LOG ENTRY ALREADY EXISTS #
                if row['file'] == '*':
                    str_sql = """
                    SELECT * FROM sys_log_dataload
                    WHERE source = '{}'
                    AND dest = '{}'
                    AND timestamp >= '{}' AND timestamp < '{}'
                    """.format(row['source'], row['dest'], str_date_from, str_date_to)
                    df_log = pd.read_sql(str_sql, conn)
                else:
                    str_sql = """
                    SELECT * FROM sys_log_dataload
                    WHERE source = '{}'
                    AND dest = '{}'
                    AND file = '{}'
                    AND timestamp >= '{}' AND timestamp < '{}'
                    """.format(row['source'], row['dest'], row['file'], str_date_from, str_date_to)
                    df_log = pd.read_sql(str_sql, conn)

                if len(df_log) < 1:  # There should be at least 1 log entry for the day.
                    # TODO: If there are N files for a source, to check for N files, not just 1 file. The difficulty is that 'file' value sometimes contains a variable (eg: date), for certain data sources!
                    # Currently, if 1 out of 2 files from the SAME source to be loaded fails, the system will PASS this!
                    # Alternatively, in scheduler_load_data.py, call each of the 3 Opera files one-by-one directly thru load_otb().
                    df_out = df_out.append(row, ignore_index=True)

    if len(df_out) > 0:
        # There are some scheduled data loads without the corresponding entries in the log table. Implying a data load error has happened.
        df_out = df_out[['source', 'dest', 'file', 'time_from', 'time_to']]  # Columns go out of order during append.
        str_msg = """
        It appears that one or more of your scheduled data loads has failed!
        See the list below for details of which scheduled loads have problems.
        """
        str_msg2 = """
        Check tables "sys_cfg_dataload_sched" and "sys_log_dataload_freq", to see why a scheduled data load in the former,
        is not present in the latter table. Successful data loads should always be logged. 
        """
        str_subject = '[fehdw_admin] Error - Data Loading Failed'
        arb = AdminReportBot()
        arb.send(str_listname='fehdw_admin', str_subject=str_subject, df=df_out, str_msg=str_msg, str_msg2=str_msg2)


def get_datarun_sched(run_id, conn):
    """ Given a run_id key, read config table to get a time range.
    For use in controlling when scheduled jobs are supposed to run.
    :param run_id:
    :param conn: DB connection.
    :return: 2 Time objects.
    """
    str_sql = """
    SELECT * FROM sys_cfg_datarun_sched
    WHERE run_id = '{}'
    """.format(run_id)
    df = pd.read_sql(str_sql, conn)

    if len(df) > 0:  # Should be at least 1 record. We'll just take the first one.
        time_from = df['time_from'][0]
        time_to = df['time_to'][0]

        t_from = dt.time(int(time_from[:2]), int(time_from[2:]))
        t_to = dt.time(int(time_to[:2]), int(time_to[2:]))

        return t_from, t_to

def get_dataload_sched(source, dest, file, conn):
    """ Given a source/dest/file key, read config table to get a time range.
    For use in controlling when scheduled jobs are supposed to run.
    :param source:
    :param dest:
    :param file:
    :param conn: DB connection.
    :return: 2 Time objects.
    """
    str_sql = """
    SELECT * FROM sys_cfg_dataload_sched
    WHERE source = '{}'
    AND dest = '{}'
    AND file = '{}' 
    """.format(source, dest, file)
    df = pd.read_sql(str_sql, conn)

    if len(df) > 0:  # Should be at least 1 record. We'll just take the first one.
        time_from = df['time_from'][0]
        time_to = df['time_to'][0]

        t_from = dt.time(int(time_from[:2]), int(time_from[2:]))
        t_to = dt.time(int(time_to[:2]), int(time_to[2:]))

        return t_from, t_to


def split_date(dt_date=dt.datetime.today(), days=1):
    """ Convenience function. Given a date, and a number of days, returns 2 formatted strings containing the 2 dates
    of format 'YYYY-MM-DD'. For use in SQL queries (on the timestamp fields). Note the default values!
    :param dt_date:
    :param days:
    :return: (str_date_from, str_date_to)
    """
    str_date_from = dt.datetime.strftime(dt_date, format('%Y-%m-%d'))
    str_date_to = dt.datetime.strftime(dt_date + dt.timedelta(days=days), format('%Y-%m-%d'))
    return(str_date_from, str_date_to)


def dec_err_handler(retries=0):
    """
    Decorator function to handle logging and retries.
    Usage: Call without the retries parameter to have it log exceptions only.
    Assumptions: 1) args[0] is "self", and "self.logger" has been instantiated.
    Ref: https://stackoverflow.com/questions/11731136/python-class-method-decorator-with-self-arguments
    :retries: Number of times to retry, in addition to original try.
    """
    def wrap(f):  # Doing the wrapping here. Called during the decoration of the function.
        def wrapped_err_handler(*args, **kwargs):  # This way, kwargs can be handled too.
            logger = args[0].logger  # args[0] is intended to be "self". Assumes that self.logger has already been created on __init__.

            if not isinstance(logger, logging.Logger):  # Ensures that a Logger object is provided.
                print('[ERROR] Please provide an instance of class: logging.Logger')
                sys.exit('[ERROR] Please provide an instance of class: logging.Logger')

            for i in range(retries + 1):  # First attempt 0 does not count as a retry.
                try:
                    if i > 0:
                        logger.info(f'[RETRYING] {f.__name__}: {i}/{retries}')  # Print number of retries, BEFORE running f() again.
                    f(*args, **kwargs)  # PAYLOAD FUNCTION.
                    break  # So you don't run f() multiple times!
                except MaxDataLoadException as ex:
                    logger.error(ex)
                    break  # Do not retry multiple times, if problem was due to this Exception.
                except Exception as ex:
                    # To only log exceptions.
                    logger.error(ex)
                    time.sleep(2 ** i)  # Exponential backoff. Pause processing for an increasing number of seconds, with each error.

        wrapped_err_handler.__name__ = f.__name__  # Nicety. Rename the error handler function name to that of the wrapped function.
        return wrapped_err_handler
    return wrap


def get_latest_file(str_folder=None, pattern=None):
    """
    Given a folder, return the last updated file in that folder.
    If pattern (regex) is given, apply pattern as a filter first.
    """
    _, _, l_files = next(os.walk(str_folder))  # First, always get all files in the dir.

    # Apply pattern to filter l_files if pattern exists #
    if pattern is not None:
        l_files = [f for f in l_files if re.search(pattern, f)]
        if len(l_files) == 0: raise Exception('No files found that match the given pattern.')

    # Get last modified file, from the filtered list #
    dt_prev = None  # Initialize outside the loop.
    for file in l_files:
        str_fn_curr = os.path.join(str_folder, file)
        dt_curr = dt.datetime.fromtimestamp(os.path.getmtime(str_fn_curr))

        if dt_prev is None:
            dt_prev = dt_curr
            str_fn_prev = str_fn_curr
        else:
            if dt_curr > dt_prev:  # Keep the most recent datetime value.
                dt_prev = dt_curr
                str_fn_prev = str_fn_curr
    return (str_fn_prev, file)


def get_files(str_folder=None, pattern=None, latest_only=False):
    """ Given a directory name, return all full filenames that exist there, and which match the pattern. Can search for latest filename only.
    :param str_folder: Directory to search for files.
    :param pattern: A regex expression, to filter the list of files.
    :param latest_only: True, if you want to get the latest filename only.
    :return: Returns a tuple of (<full filename>, <filename>) if latest_only=True. Otherwise, returns a list of tuples of (<full filename>, <filename>).
    """
    _, _, l_files = next(os.walk(str_folder))  # First, always get all files in the dir.

    # Simple case. Retrieve all files that match the pattern.
    if (str_folder is not None) & ~latest_only:
        if pattern is None:  # Return all files in the directory
            return l_files
        else:  # Return only files which match pattern.
            return [(os.path.join(str_folder, fn), fn) for fn in l_files if re.search(pattern, fn)]
    else:
        return get_latest_file(str_folder=str_folder, pattern=pattern)  # Note: The function will return a 2-values tuple!


def archive_logs(truncate=False):
    """ Archives the logs into a ZIP file, with current datetime in the filename. Existing *.log files all truncated (reinitialized).
    :param truncate: If True, will truncate existing log files.
    :return: NA
    """
    import feh.datareader  # careful to avoid circular imports!

    dr = feh.datareader.DataReader()
    str_log_dir = os.path.join(dr.config['global']['global_root_folder'], 'logs')
    str_log_archive_dir = os.path.join(dr.config['global']['global_root_folder'], 'logs', 'archive')
    str_fn_zip = 'log_' + dt.datetime.strftime(dt.datetime.today(), format='%Y%m%d_%H%M') + '.zip'
    str_fn_zip_full = os.path.join(str_log_archive_dir, str_fn_zip)

    l_files = get_files(str_folder=str_log_dir, pattern='.log$')  # Take all *.log files.

    # ZIP AND ARCHIVE THE LOG FILES #
    if len(l_files):
        with zipfile.ZipFile(str_fn_zip_full, 'w') as f_zip:  # f_zip will close itself, given the "with" construct.
            for (str_fn_full, str_fn) in l_files:
                f_zip.write(filename=str_fn_full, arcname=str_fn)  # Add file to ZIP archive.

    # TRUNCATE ALL EXISTING LOG FILES #
    if truncate:
        for (str_fn_full, str_fn) in l_files:
            open(str_fn_full, 'w').close()


def db_truncate_tables():
    """ Truncate all tables specified in the list below.
    """
    import feh.datareader  # careful to avoid circular imports!

    dr = feh.datareader.DataReader()
    dr._init_logger(logger_name='global')  # Global log only.

    l_tabs = ['stg_ezrms_forecast', 'stg_fwk_otb', 'stg_fwk_proj', 'stg_op_act_nonrev', 'stg_op_act_rev',
              'stg_op_otb_nonrev', 'stg_op_otb_rev', 'stg_otai_rates']
    for tab in l_tabs:
        str_sql = f"TRUNCATE {tab};"
        pd.io.sql.execute(str_sql, dr.db_conn)
        dr.logger.info(f'Truncated database table: {tab}')


def get_db_table_info(conn=None, schema='fehdw', filename=sys.stdout):
    """ Given a database connection and a schema, dumps useful table and column information.
    To quickly get some information about what a specific table is for, or to get the column specifications.
    """
    if filename != sys.stdout:
        fh = open(filename, 'w',
                  encoding='utf-8')  # Default is cp1252 on Windows. Use locale.getpreferredencoding(False)
    else:
        fh = sys.stdout

    # Retrieve data from MySQL DB #
    str_sql = """
    SELECT TABLE_NAME, TABLE_ROWS, CREATE_TIME, UPDATE_TIME, TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '{}'
    AND TABLE_TYPE = 'BASE TABLE'
    """.format(schema)
    df_tabs = pd.read_sql(str_sql, conn)
    df_tabs.columns = [x.lower() for x in df_tabs.columns]

    str_sql = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{}'
    """.format(schema)
    df_cols = pd.read_sql(str_sql, conn)
    df_cols.columns = [x.lower() for x in df_cols.columns]

    df_merge = pd.merge(df_tabs, df_cols, on='table_name')
    df_t = df_merge.groupby(by=['table_name']).size().rename('col_count').reset_index().copy()
    df_merge = pd.merge(df_merge, df_t, on='table_name')

    # Write to file #
    for tab, df in df_merge.groupby(by=['table_name']):
        sr_row = df.iloc[0]  # Just take the first row as it is representative. All the column values are the same.
        print('===> TABLE: {}'.format(tab), file=fh)

        if sr_row['table_comment'] != '':
            print('Description: {}'.format(sr_row['table_comment']), file=fh)

        print('Row_count: {} | Column_count: {} | Updated: {} | Created: {}'.format(sr_row['table_rows'],
                                                                                    sr_row['col_count'],
                                                                                    sr_row['update_time'],
                                                                                    sr_row['create_time']), file=fh)
        print('COLUMNS', file=fh)
        for i in df.index:
            print('col_name: {} (Type: {})'.format(df.loc[int(i), 'column_name'], df.loc[int(i), 'data_type']), file=fh)
        print('\n', file=fh)


