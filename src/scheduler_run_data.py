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
dt_date = dt.datetime.today()
str_date = dt.datetime.strftime(dt_date, format='%Y-%m-%d')

DEBUG = False  # SET BACK TO FALSE AFTER TESTING!

# NOTE: METHOD CALLS ARE PLACED IN DEPENDENCY SEQUENCE; DO NOT CHANGE! #

# RUN TIME: 0700-0730 HRS #
# proc_hotel_price_rank
# proc_op_otb_with_allot
# proc_op_otb_with_allot_diff
# proc_hotel_price_otb_evolution

# run_id: proc_hotel_price_rank #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_hotel_price_rank', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):
    if otai_drun.has_been_loaded(source='otai', dest='mysql', file='rates'):
        otai_drun.proc_hotel_price_rank(dt_date=dt_date)
    else:
        otai_drun.logger.error('Missing data load for {} data for snapshot_dt: {}.'.format('OTAI Rates', str_date))

# run_id: proc_op_otb_with_allot #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_op_otb_with_allot', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):  # Not able to do has_been_loaded() check for Opera sources. Should have no impact.
    op_drun.proc_op_otb_with_allot(dt_date=dt_date)

# run_id: proc_op_otb_with_allot_diff #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_op_otb_with_allot_diff', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):
    op_drun.proc_op_otb_with_allot_diff(dt_date=dt_date)

# run_id: proc_hotel_price_otb_evolution #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_hotel_price_otb_evolution', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):
    otai_drun.proc_hotel_price_otb_evolution(dt_date=dt_date)


# RUN TIME: 0930-1000 HRS #
# proc_occ_forecasts
# proc_occ_forecast_ezrms
# proc_occ_forecast_ezrms_diff
# proc_occ_forecast_mkt_diff

# run_id: proc_occ_forecasts #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_occ_forecasts', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):
    of_drun.proc_occ_forecasts(dt_date=dt_date)

# run_id: proc_occ_forecast_ezrms #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_occ_forecast_ezrms', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):
    of_drun.proc_occ_forecast_ezrms(dt_date=dt_date)

# run_id: proc_occ_forecast_ezrms_diff #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_occ_forecast_ezrms_diff', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):
    of_drun.proc_occ_forecast_ezrms_diff(dt_date=dt_date)

# run_id: proc_occ_forecast_mkt_diff #
t_from, t_to = feh.utils.get_datarun_sched(run_id='proc_occ_forecast_mkt_diff', conn=d_run.conn_fehdw)
if DEBUG | (t_from <= TIME_NOW < t_to):
    of_drun.proc_occ_forecast_mkt_diff(dt_date=dt_date)



# CHECK IF DATA LOAD HAS COMPLETED SUCCESSFULLY #
# Technique: In each half hour time slot, the code above will load data, and must create a log entry if successful.
# This check uses the presence of a corresponding log entry to determine if a data load is successful.
# If unsuccessful, an email is sent to the mailing list "fehdw_admin". Comment out below line to disable, if necessary.
# If successful, an email is sent to the Revenue Manager mailing list, to inform them that the data mart is ready to be used.

#feh.utils.check_datarun_not_logged(TIME_NOW, d_run.conn_fehdw)

