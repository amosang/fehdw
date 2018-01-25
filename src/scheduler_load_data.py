###################
# Purpose: Windows Task Scheduler to call this file at certain intervals (eg: 0.5 hours).
# All data loading routines to be put here, for ease of maintenance. For errors, look up respective logs.

# Data Access Window. Because SAS server has jobs which move the files, there is a specific agreed data window within
# which to copy the files.
###################
from feh.datareader import OperaDataReader
import datetime as dt

TIME_NOW = dt.datetime.now().time()  # Jobs to run within specific time windows

# Opera #
# Data Access Window: 0520-0600 hrs
if dt.time(5, 20) <= TIME_NOW <= dt.time(6):
    odr = OperaDataReader()
    odr.load()




# FWK, Rategain #
# Data Access Window: 0230-0400 hrs


