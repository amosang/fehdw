# Purpose: This script allows the user to input a snapshot_dt, and the system will drop and reload ALL
# corresponding records in the staging tables and data marts.
# It is meant to be used for recovery purposes, in the event that data sources were unavailable, and so on.

import datetime as dt
import sys
from feh.datareader import DataReader
from feh.datarunner import DataRunner

##### CHANGE THIS TO THE SNAPSHOT_DT TO BE DELETED! #####
STR_SNAPSHOT_DT_TO_DEL = '2018-05-18'  # The snapshot_dt to be deleted. Format: "YYYY-MM-DD"
###########

try:
    dt_snapshot_dt = dt.datetime.strptime(STR_SNAPSHOT_DT_TO_DEL, '%Y-%m-%d')

    # Does not make sense for snapshot_dt to be greater than current date. To halt processing if so.
    if dt_snapshot_dt.date() > dt.datetime.today().date():
        print('[ERROR] {}'.format('snapshot_dt cannot be later than today date!'))
        sys.exit()

    d_read = DataReader()
    d_read._init_logger(logger_name='datareader')
    d_run = DataRunner()

    # DROP AND RELOAD #
    d_read.drop_and_reload_staging_tables(dt_date=dt_snapshot_dt)  # Handle staging tables.
    d_run.drop_and_reload_data_marts(dt_date=dt_snapshot_dt)  # Handle data marts.

except Exception as ex:
    print('[ERROR] {}'.format(ex))



