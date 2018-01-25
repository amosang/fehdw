import pandas as pd
from pandas import DataFrame
from feh.datareader import DataReader
import datetime as dt

dr = DataReader()  # Use this for the database conn only.

# Table: sys_cfg_dataload. Policy table which controls how frequently a source can move data to destination.

df = DataFrame(columns=['source', 'dest', 'max_freq', 'freq_unit'],
                  data=[['opera', 'mysql', 1, 'd'],
                        ['otai', 'mysql', 1, 'd']
                        ])
df.to_sql('sys_cfg_dataload', dr.db_conn, index=False, if_exists='replace')


# Table: sys_log_dataload. Logs each data loading activity. All data loaders to check if the load freq limit has been
#  reached, before doing any more loading!
ts = dt.datetime.today()

df = DataFrame(columns=['source', 'dest', 'timestamp', 'files'],
                  data=[['opera', 'mysql', ts, 'C:\sample\filename.xlsx'],
                        ])
df.to_sql('sys_log_dataload', dr.db_conn, index=False, if_exists='replace')
