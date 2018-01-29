###################
# Purpose: Windows Task Scheduler to call this file at certain intervals (eg: every 30 mins).
# All data loading routines to be put here, for ease of maintenance. For errors, look up respective logs.

# Data Access Window. Because SAS server has jobs which MOVE the files, there is a specific agreed data window within
# which to copy the files.
###################
from feh.datareader import OperaDataReader, OTAIDataReader, FWKDataReader
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

# Rategain #
# Data Access Window: 0230-0400 hrs


