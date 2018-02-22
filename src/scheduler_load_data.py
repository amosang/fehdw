###################
# Purpose: Windows Task Scheduler to run this file at specific intervals (eg: every 30 mins). Working assumption is 30 mins.
# All data loading routines to be put here in 1 place, for ease of maintenance. For errors, look up respective logs.

# Data Access Window. Because SAS server has jobs which MOVE the files, there is a specific agreed data window within
# which to copy the files.
###################
from feh.datareader import DataReader, OperaDataReader, OTAIDataReader, FWKDataReader, EzrmsDataReader
import feh.utils
import datetime as dt

dr = DataReader()  # Need this for the DB conn.
TIME_NOW = dt.datetime.now().time()  # Jobs to run within specific time windows

# FWK #
# Data Access Window: 0200-0430 hrs
t_from, t_to = feh.utils.get_dataload_sched(source='fwk', dest='mysql', file='*', conn=dr.db_conn)
if t_from <= TIME_NOW < t_to:
    fwk_dr = FWKDataReader()
    fwk_dr.load()

# Opera #
# Data Access Window: 0520-0600 hrs
t_from, t_to = feh.utils.get_dataload_sched(source='opera', dest='mysql', file='*', conn=dr.db_conn)
if t_from <= TIME_NOW < t_to:
    op_dr = OperaDataReader()
    op_dr.load()

# OTA Insights #
# Data Access Window: All day. Note that data scrapping on their end happens between 0000-0600 hrs, local hotel time.
t_from, t_to = feh.utils.get_dataload_sched(source='otai', dest='mysql', file='rates', conn=dr.db_conn)
if t_from <= TIME_NOW < t_to:
    otai_dr = OTAIDataReader()
    otai_dr.load()

# EzRMS #
# Data Access Window: After 0930 hrs. Avoid simultaneous run with the other one.
t_from, t_to = feh.utils.get_dataload_sched(source='ezrms', dest='mysql', file='forecast', conn=dr.db_conn)
if t_from <= TIME_NOW < t_to:
    ezrms_dr = EzrmsDataReader()
    ezrms_dr.load()

# CHECK IF DATA LOAD HAS COMPLETED SUCCESSFULLY #
# Technique: In each half hour time slot, the code above will load data, and must create a log entry if successful.
# This check uses the presence of a corresponding log entry to determine if a data load is successful.
# If unsuccessful, an email is sent to the mailing list "fehdw_admin". Comment out below line to disable, if necessary.
feh.utils.check_dataload_not_logged(TIME_NOW, dr.db_conn)

