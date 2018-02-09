###################
# Purpose: Windows Task Scheduler to run this file at specific intervals (eg: every 30 mins). Working assumption = 30 mins.
# All data loading routines to be put here in 1 place, for ease of maintenance. For errors, look up respective logs.

# Data Access Window. Because SAS server has jobs which MOVE the files, there is a specific agreed data window within
# which to copy the files.
###################
from feh.datareader import OperaDataReader, OTAIDataReader, FWKDataReader, EzrmsDataReader
import datetime as dt

TIME_NOW = dt.datetime.now().time()  # Jobs to run within specific time windows

# FWK #
# Data Access Window: 0200-0430 hrs
if dt.time(3, 0) <= TIME_NOW < dt.time(3, 30):
    fwk_dr = FWKDataReader()
    fwk_dr.load()

# Opera #
# Data Access Window: 0520-0600 hrs
if dt.time(5, 20) <= TIME_NOW < dt.time(6):
    op_dr = OperaDataReader()
    op_dr.load()

# OTA Insights #
# Data Access Window: All day. Note that data scrapping on their end happens between 0000-0600 hrs, local hotel time.
if dt.time(6, 30) <= TIME_NOW < dt.time(7):
    otai_dr = OTAIDataReader()
    otai_dr.load()

# EzRMS #
# Data Access Window: After 0930 hrs. Avoid simultaneous run with the other one.
if dt.time(9, 30) <= TIME_NOW < dt.time(10):
    ezrms_dr = EzrmsDataReader()
    ezrms_dr.load()


