import datetime
import functools
import pandas as pd
import os
import logging
from configobj import ConfigObj
from pandas import DataFrame
from feh.utils import dec_err_handler

class DataReader(object):
    config = ConfigObj('fehdw.conf')

    def __init__(self):
        pass

    @classmethod
    def print_cfg(cls):
        for key, value in cls.config.iteritems():
            print(key, value, '\n')

    def read(self, str_filename):
        ''' Reads the filename/data source and saves the data frame as a class attribute.
        Each subclass is responsible for handling the idiosyncrasies of its respective data source.
        :param str_filename:
        :return: DataFrame
        '''
        raise NotImplementedError

    def read_all(self, str_dir):
        """ Given a directory name, read all the files (of a certain type) in that directory.

        Results are saved as a class attribute.
        :return: DataFrame
        """
        raise NotImplementedError

    def to_excel(self, filename):
        """ Writes the df_data DataFrame contents to Excel.

        Reference: https://stackoverflow.com/questions/29463274/simulate-autofit-column-in-xslxwriter; http://xlsxwriter.readthedocs.io/example_pandas_column_formats.html,
        """
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        self.df_data.to_excel(writer, index=False)  # Creates the Worksheet='Sheet1'.

        # Set column widths to be same length as column names (so that will display column names) #
        # Get the xlsxwriter worksheet object.
        worksheet = writer.sheets['Sheet1']

        l_col_lengths = [len(x) for x in self.df_data.columns]
        for i, width in enumerate(l_col_lengths):
            worksheet.set_column(i, i, width)
        writer.save()
        writer.close()


class OperaDataReader(DataReader):
    """ For reading Opera files, which were output by Opera Simple Report Writer.

    The data is read into a class attribute as a DataFrame.

    """
    df_data = DataFrame()  # Instance attribute which holds the data read in, if any.
    # logger -> instance variable.

    def __init__(self):
        # LOGGING #
        self.logger = logging.getLogger('opera_datareader')
        if self.logger.hasHandlers():  # Clear existing handlers, else will have duplicate logging messages.
            self.logger.handlers.clear()
        # Create the handler for the main logger
        str_fn_logger = os.path.join(self.config['global']['global_root_folder'], self.config['data_sources']['opera']['logfile'])
        fh_logger = logging.FileHandler(str_fn_logger)
        str_format = '[%(asctime)s] - [%(levelname)s] - %(message)s'
        fh_logger.setFormatter(logging.Formatter(str_format))
        self.logger.addHandler(fh_logger)  # Add the handler to the base logger
        self.logger.setLevel(logging.INFO)  # By default, logging will start at 'WARNING' unless we tell it otherwise.

    def __del__(self):
        # Logging. Close all file handlers to release the lock on the open files.
        handlers = self.logger.handlers[:]  # https://stackoverflow.com/questions/15435652/python-does-not-release-filehandles-to-logfile
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)

    @dec_err_handler(retries=3)
    def test_decorator(self, x, y):
        print(f'{x} divided by {y} is {x / y}')

    def read(self, read_type='one', str_fn_60_new=None, str_fn_61_new=None):  # Read what? one/all/latest
        self.df_data = DataFrame()  # Clear instance variable each time.

        if read_type == 'one':  # Read just one file
            # str_date = datetime.datetime.today().strftime('%d%b%Y').upper()  # Take filename with today's date.
            str_date = '02JAN2018'  # DEBUG - HARDCODED!

            str_fn_60 = 'OP_RES_{}_AM_60.xlsx'.format(str_date)
            str_fn_61 = 'OP_RES_{}_AM_61.xlsx'.format(str_date)

            str_folder = self.config['data_sources']['opera']['root_folder']  # Get from config file.
            str_fn_60 = os.path.join(str_folder, str_fn_60)
            str_fn_61 = os.path.join(str_folder, str_fn_61)  # smaller file

            # Allows file names to be manually overwritten, for ad hoc use.
            if str_fn_60_new:
                str_fn_60 = str_fn_60_new
            if str_fn_61_new:
                str_fn_61 = str_fn_61_new

            # READ THE FILES #
            df_reservation = pd.read_excel(str_fn_61, sheet_name='Reservation', keep_default_na=False, na_values=[' '])
            df_allotment = pd.read_excel(str_fn_61, sheet_name='Allotment', keep_default_na=False, na_values=[' '])
            df_arr_depart = pd.read_excel(str_fn_61, sheet_name='Arr Dept', keep_default_na=False, na_values=[' '])
            df_sales = pd.read_excel(str_fn_61, sheet_name='Sales', keep_default_na=False, na_values=[' '])
            df_guest_profile = pd.read_excel(str_fn_61, sheet_name='Guest profile', keep_default_na=False, na_values=[' '])
            df_no_of_guest = pd.read_excel(str_fn_61, sheet_name='No of guest', keep_default_na=False, na_values=[' '])

            df_rev_vhac_vhb = pd.read_excel(str_fn_61, sheet_name='Revenue VHAC VHB', keep_default_na=False, na_values=[' '])
            df_rev_vhc_vhk = pd.read_excel(str_fn_61, sheet_name='Revenue VHC VHK', keep_default_na=False, na_values=[' '])
            df_rev_ohs_tqh = pd.read_excel(str_fn_61, sheet_name='Revenue OHS TQH', keep_default_na=False, na_values=[' '])
            df_rev_oph_tes = pd.read_excel(str_fn_61, sheet_name='Revenue OPH TES', keep_default_na=False, na_values=[' '])
            df_rev_rhs_amoy = pd.read_excel(str_fn_61, sheet_name='Revenue RHS AMOY', keep_default_na=False, na_values=[' '])

            # Renaming columns manually. Assumes that source Excel files do not change column positions! #
            df_reservation.columns = ['confirmation_no', 'resort', 'res_reserv_status', 'res_bkdroom_ct_lbl',
                                      'res_roomty_lbl', 'res_reserv_date',
                                      'res_cancel_date', 'res_pay_method', 'res_credcard_ty']

            df_allotment.columns = ['confirmation_no', 'resort', 'allot_allot_book_cde', 'allot_allot_book_desc']

            df_arr_depart.columns = ['confirmation_no', 'resort', 'arr_arrival_dt', 'arr_departure_dt', 'arr_arrival_time',
                                     'arr_departure_time']

            df_sales.columns = ['confirmation_no', 'resort', 'sale_comp_name', 'sale_zip_code', 'sale_sales_rep',
                                'sale_sales_rep_code',
                                'sale_industry_code', 'sale_trav_agent_name', 'sale_sales_rep1', 'sale_sales_rep_code1',
                                'sale_all_owner']

            df_guest_profile.columns = ['confirmation_no', 'resort', 'gp_guest_ctry_cde', 'gp_nationality',
                                        'gp_origin_code', 'gp_guest_lastname',
                                        'gp_guest_firstname', 'gp_guest_title_code', 'gp_spcl_req_desc', 'gp_email',
                                        'gp_dob', 'gp_vip_status_desc']

            df_no_of_guest.columns = ['confirmation_no', 'resort', 'ng_adults', 'ng_children', 'ng_extra_beds', 'ng_cribs']

            l_rev_cols = ['confirmation_no', 'resort', 'business_dt', 'rev_marketcode', 'rev_ratecode', 'rev_sourcecode',
                          'rev_sourcename', 'rev_eff_rate_amt', 'rev_actual_room_nts', 'rev_rmrev_extax',
                          'rev_food_rev_inctax',
                          'rev_oth_rev_inctax', 'rev_hurdle', 'rev_hurdle_override', 'rev_restriction_override']

            df_rev_ohs_tqh.columns = l_rev_cols
            df_rev_oph_tes.columns = l_rev_cols
            df_rev_rhs_amoy.columns = l_rev_cols
            df_rev_vhac_vhb.columns = l_rev_cols
            df_rev_vhc_vhk.columns = l_rev_cols

            # Merge all the Revenue dfs. They were originally separated due to bandwidth limitations of the "Vision" extraction tool.
            df_merge_rev = pd.concat([df_rev_ohs_tqh, df_rev_oph_tes, df_rev_rhs_amoy, df_rev_vhac_vhb, df_rev_vhc_vhk])
            df_merge_rev['snapshot_dt'] = datetime.datetime.today()

            # Merge all the non-Revenue dfs, using Reservation as the base. They are said to be of 1-to-1 relationship.
            l_dfs = [df_reservation, df_allotment, df_arr_depart, df_sales, df_guest_profile, df_no_of_guest]
            df_merge = functools.reduce(lambda left, right: pd.merge(left, right, on=['confirmation_no', 'resort']), l_dfs)

        elif read_type == 'all':
            pass

        return self.df_data


