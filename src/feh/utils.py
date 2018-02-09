import pandas as pd
import sys
import logging
import os
import re
import datetime as dt
import time
import zipfile
import feh.datareader  # careful to avoid circular imports!


class MaxDataLoadException(Exception):
    pass


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
                    f(*args, **kwargs)
                    #print('No exception encountered')
                    break  # So you don't run f() multiple times!
                except MaxDataLoadException as ex:
                    logger.error(ex)
                    break  # Do not retry multiple times, if problem was due to this Exception.
                except Exception as ex:
                    # To only log exceptions.
                    #print('Exception: ' + str(ex))
                    if i > 0:
                        logger.info(f'[RETRYING] {f.__name__}: {i}/{retries}')
                        time.sleep(2 ** i)  # Exponential backoff. Pause processing for an increasing number of seconds, with each error.
                    logger.error(ex)

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
    dr = feh.datareader.DataReader()
    str_log_dir = os.path.join(dr.config['global']['global_root_folder'], 'logs')
    str_log_archive_dir = os.path.join(dr.config['global']['global_root_folder'], 'logs', 'archive')
    str_fn_zip = 'log_'+dt.datetime.strftime(dt.datetime.today(), format='%Y%m%d_%H%M')+'.zip'
    str_fn_zip_full = os.path.join(str_log_archive_dir, str_fn_zip)

    l_files = get_files(str_folder=str_log_dir, pattern='.log$')  # Take only *.log files.

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
    """ Truncate all tables in the list below.
    """
    dr = feh.datareader.DataReader()
    dr._init_logger(logger_name='global')  # Global log only.

    l_tabs = ['stg_ezrms_forecast', 'stg_fwk_otb', 'stg_fwk_proj', 'stg_op_act_nonrev', 'stg_op_act_rev',
              'stg_op_otb_nonrev', 'stg_op_otb_rev', 'stg_otai_rates']
    for tab in l_tabs:
        str_sql = f"TRUNCATE {tab};"
        pd.io.sql.execute(str_sql, dr.db_conn)
        dr.logger.info(f'Truncated database table: {tab}')
