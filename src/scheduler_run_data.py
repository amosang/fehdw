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

of_drun = OccForecastDataRunner()
op_drun = OperaOTBDataRunner()
otai_drun = OTAIDataRunner()
TIME_NOW = dt.datetime.now().time()  # Jobs to run within specific time windows

# OTAI #
# Run time: 0700-0730 hrs
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_hotel_price_rank', conn=d_run.conn_fehdw)
if t_from <= TIME_NOW < t_to:
    otai_drun = OTAIDataRunner()
    otai_drun.run()

# CHECK IF DATA LOAD HAS COMPLETED SUCCESSFULLY #
# Technique: In each half hour time slot, the code above will load data, and must create a log entry if successful.
# This check uses the presence of a corresponding log entry to determine if a data load is successful.
# If unsuccessful, an email is sent to the mailing list "fehdw_admin". Comment out below line to disable, if necessary.
# If successful, an email is sent to the Revenue Manager mailing list, to inform them that the data mart is ready to be used.
#feh.utils.check_dataload_not_logged(TIME_NOW, d_run.conn_fehdw)

