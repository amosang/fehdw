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
    Usage: Call without the retries parameter to have it log exceptions only, without retrying.

    Assumptions:
    1) args[0] is "self", and 2) "self.logger" has been instantiated. This means that this decorator will not work with regular functions!

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
                    logger.error(ex)  # Logging all uncaught exceptions from the called function/method.
                    time.sleep(2 ** i)  # Exponential backoff. Pause processing for an increasing number of seconds, with each error.

        wrapped_err_handler.__name__ = f.__name__  # Nicety. Rename the error handler function name to that of the wrapped function.
        return wrapped_err_handler
    return wrap


def _send_email_data_imputation(str_listname, str_subject, l_tab_name):
    """ Sends email pertaining to data imputation.
    :return: NA
    """
    arb = AdminReportBot()

    str_msg = """       
    Hello! Just to inform you that I have patched your data feed from the above source system, because the data sources for that source system cannot be accessed for some reason. <br> 
    This means that I have copied the last successfully loaded copy of this data and used it in place of today's missing data (for that source system only). <br>
    The ORCA sys admins have been notified and will be looking into this. <br>  
    Note: Data patching, by design, will only happen for the Opera/EzRMS/FWK data sources. <br><br><br>
    
    <strong>Technical info for sys admins</strong> <br>
    The following tables have been patched: <br>
    {} <br><br>
    
    Check the datareader "global.log" file for more details.
    """.format(str(l_tab_name))
    arb.send(str_listname=str_listname, str_subject=str_subject, df=None, str_msg=str_msg, str_msg2='')


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
    str_date_from, str_date_to = split_date()  # today's date.

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

    # # DEBUG. Uncomment this to simulate error for "ezrms" #
    # str_sql = """
    # SELECT * FROM sys_cfg_dataload_sched WHERE source = 'ezrms'
    # """
    # df_out = pd.read_sql(str_sql, conn)
    # # ENDDEBUG #

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

        # TODO: For some reason, not able to send 2 emails quickly back-to-back. Sleeping does not help either.
        # Will get below error. This is somewhat mitigated, because receiving either email let's the admins know something has happened.
        # "ERROR:admin_report_bot:[WinError 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond"

        # If data load fails and the source is EzRMS/Opera/FWK, copy the last successful data load but with a new snapshot_dt #
        str_listname = 'rm_im_all'  # Send to this mailing list.
        for idx, row in df_out.iterrows():
            str_subject = '[{}] Data Patching has been done for: {}'.format(str_listname, row['source'].upper())

            # EzRMS Forecast #
            if (row['source'] == 'ezrms') & (row['dest'] == 'mysql') & (row['file'] == 'forecast'):
                l_tab_name = ['stg_ezrms_forecast']
                copy_last_snapshot_dt_dataset(l_tab_name=l_tab_name, row=row)
                _send_email_data_imputation(str_listname=str_listname, str_subject=str_subject, l_tab_name=l_tab_name)

            # Opera #
            if (row['source'] == 'opera') & (row['dest'] == 'mysql') & (row['file'] == '*'):
                l_tab_name = ['stg_op_act_nonrev', 'stg_op_act_rev', 'stg_op_cag', 'stg_op_otb_nonrev', 'stg_op_otb_rev']
                copy_last_snapshot_dt_dataset(l_tab_name=l_tab_name, row=row)
                _send_email_data_imputation(str_listname=str_listname, str_subject=str_subject, l_tab_name=l_tab_name)

            # FWK #
            if (row['source'] == 'fwk') & (row['dest'] == 'mysql') & (row['file'] == '*'):
                l_tab_name = ['stg_fwk_proj', 'stg_fwk_otb']
                copy_last_snapshot_dt_dataset(l_tab_name=l_tab_name, row=row)
                _send_email_data_imputation(str_listname=str_listname, str_subject=str_subject, l_tab_name=l_tab_name)


def check_datarun_not_logged(t_timenow, conn):
    """ This is called from scheduler_run_data.py. Given a Time, iterate through the scheduled data run table to see
    which jobs are supposed to have run, then look up the data run logs table to look for corresponding entries.
    If corresponding entries do not exist, this means that the run must have failed.

    TODO: Note that logging does not happen for any of the functions in feh.utils (because cannot use decorator func as there is no "logger"). Might wish to refactor some methods into the DataRun class instead?
    :param t_timenow:
    :param conn: Connection object to 'fehdw' database.
    :return: NA
    """
    df_out_err = DataFrame()
    df_out_ok = DataFrame()
    str_date_from, str_date_to = split_date()  # today's date.

    str_sql = """
    SELECT * FROM sys_cfg_datarun_sched ORDER BY seq
    """
    df_sched = pd.read_sql(str_sql, conn)

    if len(df_sched) > 0:  # Iterate thru df_sched to see which (run_id + snapshot_dt) rows fall within the time_from--time_to range.
        for idx, row in df_sched.iterrows():  # This works even if there are N run_ids for a scheduled time slot, because we iterate through.
            time_from = row['time_from']
            time_to = row['time_to']
            t_from = dt.time(int(time_from[:2]), int(time_from[2:]))
            t_to = dt.time(int(time_to[:2]), int(time_to[2:]))
            # For each scheduled task that is supposed to have run, check if log entry exists, indicating successful run #
            # There will not be repeated alert emails sent, because the monitoring happens immediately after the job which generates the logs, both of which run in the same half hour time window!
            if t_from <= t_timenow < t_to:
                # CHECK IF LOG ENTRY ALREADY EXISTS #
                str_sql = """
                SELECT * FROM sys_log_datarun
                WHERE run_id = '{}'
                AND timestamp >= '{}' AND timestamp < '{}'
                """.format(row['run_id'], str_date_from, str_date_to)
                df_log = pd.read_sql(str_sql, conn)

                if len(df_log) < 1:  # There should be at least 1 log entry for the day.
                    df_out_err = df_out_err.append(row, ignore_index=True)
                else:
                    df_out_ok = df_out_ok.append(row, ignore_index=True)

    # Users don't want to know about the processing status of certain run_ids (eg: archive_data_marts, backup_tables), as these are system related.
    # We eliminated these run_ids from df_out_ok, but not from df_out_err, because the admins will want to know if there's an error.
    if len(df_out_ok) > 0:
        df_out_ok = df_out_ok[df_out_ok['to_notify'] == 1]

    # SEND MESSAGE TO INFORM ADMINS ABOUT ERRONEOUS RUN #
    if len(df_out_err) > 0:  # ie: There are some scheduled data runs without the corresponding entries in the log table. Implies that a data run error has happened.
        df_out_err = df_out_err[['run_id', 'time_from', 'time_to']]  # Columns go out of order during append.
        str_msg = """
        Oh no, it appears that one or more of your scheduled data runs has failed!
        See the list below for details of which scheduled runs have problems.
        """
        str_msg2 = """
        Check tables "sys_cfg_datarun_sched" and "sys_log_datarun_freq", to see why a scheduled data run in the former,
        is not present in the latter table. Successful data runs should always be logged. 
        """
        str_listname = 'fehdw_admin'
        str_subject = '[{}] Error - Data Run Failed'.format(str_listname)
        arb = AdminReportBot()
        arb.send(str_listname=str_listname, str_subject=str_subject, df=df_out_err, str_msg=str_msg, str_msg2=str_msg2)

    # SEND MESSAGE TO INFORM USERS ABOUT SUCCESSFUL RUN #
    if len(df_out_ok) > 0:  # This len check is important! It prevents an email from being sent out if no scheduled datarun was run!
        str_listname_rm_im_all = 'rm_im_all'  # To switch this back to 'rm_im_all' when LIVE.

        df_out_ok = df_out_ok[['run_id', 'time_from', 'time_to']]  # Show users only some relevant columns.

        str_msg = """
        Hello! The following scheduled data runs have been completed, and the associated data marts are ready for 
        use in your visualizations.
        """  # In the outgoing email, "df_out_ok" will appear immediately below this message.

        str_subject = '[{}] Data run completed'.format(str_listname_rm_im_all)

        # "str_msg2" variable will be constructed differently, depending on whether there are errors found (ie: df_out_err has rows).
        if len(df_out_err) > 0:
            str_subject = '[{}] Data run completed with errors'.format(str_listname_rm_im_all)

            str_msg2 = """
            Uh-oh! These data runs appear to have errors. The data marts will still load, but you might not get the latest data.
            I will go notify the admins now! 
            <br />
            {}
            """.format(df_out_err.to_html(index=False, na_rep='', justify='left'))
        else:
            str_msg2 = ''

        # This always gets sent to the users, as they should always be notified when the run has completed.
        arb = AdminReportBot()
        arb.send(str_listname=str_listname_rm_im_all, str_subject=str_subject, df=df_out_ok, str_msg=str_msg, str_msg2=str_msg2)


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


def get_latest_file(str_folder: object = None, pattern: object = None) -> object:
    """ Given a folder, return the last updated file in that folder.
    If pattern (regex) is given, apply pattern as a filter first.
    :return: A tuple containing (<str_fullpath>, <str_filename>), or (None, None) if file pattern not found.
    """
    _, _, l_files = next(os.walk(str_folder))  # First, always get all files in the dir.

    # Apply pattern to filter l_files if pattern exists #
    if pattern is not None:
        l_files = [f for f in l_files if re.search(pattern, f)]
        if len(l_files) == 0:
            #raise Exception('No files found that match the given pattern.')
            return None, None  # Let the calling method handle error logging instead.

    # Get last modified file, from the filtered list #
    dt_prev = None  # Initialize outside the loop.
    for file in l_files:
        str_fn_curr = os.path.join(str_folder, file)
        dt_curr = dt.datetime.fromtimestamp(os.path.getmtime(str_fn_curr))

        if dt_prev is None:
            dt_prev = dt_curr
            str_fullpath = str_fn_curr
            str_filename = file
        else:
            if dt_curr > dt_prev:  # Keep the most recent datetime value.
                dt_prev = dt_curr
                str_fullpath = str_fn_curr
                str_filename = file
    return str_fullpath, str_filename


def get_files(str_folder=None, pattern=None, latest_only=False):
    """ Given a directory name, return a list of all full filenames that exist there, and which match the pattern. Can search for latest filename only.
    :param str_folder: Directory to search for files.
    :param pattern: A regex expression, to filter the list of files.
    :param latest_only: True, if you want to get the latest filename only.
    :return: Returns a tuple of (<full filename>, <filename>) if latest_only=True. Otherwise, returns a list of tuples of (<str_fullpath>, <str_filename>).

    Note: Most cases will use latest_only=True. Except for the less used functions like archive_logs().
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


def convert_str_camelcase_to_snakecase(str):
    """ Given a CamelCase string, convert it to snake_case.
    Ref: https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2',
                str)  # The '\1_\2' probably refers to the 2 parts logically grouped by the pattern, using the parentheses.
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def prettify_col_names(df, camel_to_snake=True):
    """ Takes in a DataFrame, and prettifies the column names.
    Returns the same DataFrame.
    Column transformations:
    - All to lowercase
    - Spaces and periods replaced by underscore
    - convert_str_camelcase_to_snakecase()
    """
    df.columns = df.columns.str.replace('[. ]', '_')

    if camel_to_snake:
        df.columns = df.columns.to_series().reset_index(drop=True).apply(convert_str_camelcase_to_snakecase)

    df.columns = df.columns.str.lower()  # This will always run.
    return df


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
        with zipfile.ZipFile(str_fn_zip_full, 'w', compression=zipfile.ZIP_DEFLATED) as f_zip:  # f_zip will close itself, given the "with" construct.
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


def copy_last_snapshot_dt_dataset(l_tab_name, row):
    """ Given a list of table names, for each table, take the MAX(snapshot_dt). Select the data subset from that table
    based on snapshot_dt_max. Then override snapshot_dt with today(). Lastly, insert this data set back into the same table.

    Note: Logging will be only to global.log, and not to any of the sub-logs, because we wouldn't know which sub-log to use.
    :param l_tab_name: List of table names to process.
    :param row: Series data type. Contains source/dest/file. For logging to the log table.
    :return: NA
    """
    import feh.datareader  # careful to avoid circular imports!

    dr = feh.datareader.DataReader()
    dr._init_logger(logger_name='global')  # Global log only.

    for str_tab_name in l_tab_name:
        # Get the snapshot_dt of the last successful load.
        str_sql = """
        SELECT DATE(MAX(snapshot_dt)) AS snapshot_dt_max FROM {}
        """.format(str_tab_name)
        df = pd.read_sql(str_sql, dr.db_conn)

        dt_max = df['snapshot_dt_max'][0]
        str_date_from, str_date_to = split_date(dt_max)

        str_sql = """
        SELECT * FROM {} 
        WHERE snapshot_dt >= '{}' AND snapshot_dt < '{}'
        """.format(str_tab_name, str_date_from, str_date_to)
        df_tab = pd.read_sql(str_sql, dr.db_conn)
        df_tab['snapshot_dt'] = dt.datetime.today()  # Take data set as is, but replace snapshot_dt with current time.

        # WRITE TO DATABASE #
        df_tab.to_sql(str_tab_name, dr.db_conn, index=False, if_exists='append')

        # LOG DATALOAD #
        dr.logger.info('Imputed data set for table: {} using latest snapshot_dt: {}'.format(str_tab_name, str_date_from))
        dr.log_dataload(row['source'], row['dest'], row['file'])


def get_df_sample_values(df=None, threshold=10, filename=sys.stdout):
    """ Given a DataFrame, output key info about it, and its columns.
        Inputs:
        filename. Fully qualified file name string.

        Key Info includes:
        - df.head(10). Sample rows.
        - df.shape. num_row x num_cols
        - df.dtypes. data types of each col.
        - Top N values by frequency, for each column, sort in descending order of frequency count. With freq count and percent.
        - Numeric Type specific analysis. If data can be converted to numeric without errors, obtain the min/max/mean/median/SD.
        [TODO]
        - Domain-specific checks? Eg: email formats. postcode formats.
        :return: Does not return anything. Outputs to either sys.stdout or filename.
    """
    if filename != sys.stdout:
        fh = open(filename, 'w',
                  encoding='utf-8')  # Default is cp1252 on Windows. Use locale.getpreferredencoding(False)
    else:
        fh = sys.stdout

    # Output first N rows as complete sample data #
    print('=== SAMPLE ROWS ===', file=fh)
    print(df.head(10), file=fh)

    # Output shape of dataframe #
    print('\n=== SHAPE OF DATA FRAME (row x col) ===: ' + str(df.shape), file=fh)

    # Output data types of each column #
    print('\n=== COLUMN DATA TYPES ===', file=fh)
    df.info(buf=fh)

    # Output each column top N most frequent values #
    print('\n', file=fh)
    print('=== TOP n VALUES BY FREQUENCY ===', file=fh)
    if df is not None:
        for col_name in df.columns:
            print('[PROCESSING] Column: ' + col_name)  # DEBUG

            i_unique_val_count = len(
                df[col_name].unique())  # Number of unique data values, for that column. Includes NaN!
            print('COLUMN: {} | {} unique values.'.format(col_name, i_unique_val_count), file=fh)
            # print('COLUMN: {} | {} unique values.'.format(col_name, i_unique_val_count))   # DEBUG

            sr = df.groupby([col_name]).size().sort_values(ascending=False,
                                                           inplace=False, )  # Series. Note: NaN category excluded from groupby!
            df_count = sr.reset_index(name='count')
            df_count['percent'] = df_count['count'] / len(
                df)  # Divide count by total number of records (len(df)), including NaN.

            # print(df_count)  # DEBUG

            print(df_count.iloc[0:threshold, :], file=fh)  # Take N (threshold) number of rows only.

            if threshold < i_unique_val_count:
                print('[Showing {} of {} unique values]\n'.format(threshold, i_unique_val_count), file=fh)
            else:
                print('[Showing {} of {} unique values]\n'.format(i_unique_val_count, i_unique_val_count), file=fh)

    # Output each column's numeric properties, if the underlying data is potentially numeric. #
    print('=== NUMERIC PROPERTIES OF COLUMNS ===', file=fh)
    if df is not None:
        for col_name in df.columns:
            print('COLUMN: ' + col_name, file=fh)

            try:
                sr_col = pd.to_numeric(df[col_name])  # Converted to a Series of numeric type.
                if df[col_name].dtype == '<M8[ns]':  # Date Types are not numeric!
                    print('Column {} is not numeric. Is a DATE type.'.format(col_name), file=fh)
                    continue
                else:
                    print('Min: {} | Max: {} | Mean: {} | Median: {} | StdDev: {}'.format(round(sr_col.min(), 2),
                                                                                          round(sr_col.max(), 2),
                                                                                          round(sr_col.mean(), 2),
                                                                                          round(sr_col.median(), 2),
                                                                                          round(sr_col.std(), 2)),
                          file=fh)
            except ValueError:
                print('Column {} is not numeric.'.format(col_name), file=fh)  # Column cannot be converted to numeric.

    # Close the file handle properly.
    if fh != sys.stdout:
        fh.close()  # Must be a file handle, so close it.

def check_sql_table_exist(tab_name, conn):
    """ Given an SQL table name and a connection object, check if that table already exists.
    :param tab_name: Table name.
    :param conn: Connection object.
    :return: True if exists, False otherwise.
    """
    str_sql = """
    SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.tables
    WHERE TABLE_SCHEMA = 'fehdw'
    AND TABLE_NAME = '{}'
    """.format(tab_name)
    i = pd.read_sql(str_sql, conn)['count'][0]
    return False if i == 0 else True
