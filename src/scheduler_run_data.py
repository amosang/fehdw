######################################
# Purpose: Windows Task Scheduler to run this file at specific intervals (eg: every 30 mins). Working assumption is 30 mins.
# All data run routines to be put here in 1 place, for ease of maintenance.

# This script is responsible for updating all data mart tables ("dm1*", "dm2*", etc), in the correct sequence.
# It assumes that the "stg*" tables have already been updated.
######################################
from feh.datarunner import DataRunner, OccForecastDataRunner, OperaOTBDataRunner, OTAIDataRunner
import feh.utils
import datetime as dt

d_run = DataRunner()  # Need this for the DB conn.
TIME_NOW = dt.datetime.now().time()  # Jobs to run within specific time windows

# Opera #
# Data Access Window: 0530-0600 hrs
t_from, t_to = feh.utils.get_dataload_sched(source='opera', dest='mysql', file='*', conn=dr.db_conn)
if t_from <= TIME_NOW < t_to:
    op_dr = OperaDataReader()
    op_dr.load()

# CHECK IF DATA LOAD HAS COMPLETED SUCCESSFULLY #
# Technique: In each half hour time slot, the code above will load data, and must create a log entry if successful.
# This check uses the presence of a corresponding log entry to determine if a data load is successful.
# If unsuccessful, an email is sent to the mailing list "fehdw_admin". Comment out below line to disable, if necessary.
#feh.utils.check_dataload_not_logged(TIME_NOW, dr.db_conn)

