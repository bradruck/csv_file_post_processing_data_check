# pandas_manager module
# Module holds the class => PandasManager - manages the Pandas data-frame interface
# Class responsible for the Pandas data-frame creation and all pandas related functions
#
import pandas as pd
import logging


class PandasManager(object):
    def __init__(self):
        self.data_frame = pd.DataFrame()        # creates a new empty pandas data frame
        self.logger = logging.getLogger(__name__)

    # Load the contents of csv file into a pandas data-frame
    #
    def data_frame_load(self, file_name):
        with open(file_name, 'rb') as csv:
            try:
                self.data_frame = pd.read_csv(csv, sep='|')
            except Exception as e:
                self.logger.error("Data load problem, check the csv file: {} - {}".format(file_name, e))
                return None
            else:
                # Check for any missing values
                if self.data_frame.notnull().any().any():
                    return self.data_frame
                else:
                    return None

    # Returns the number of rows and columns in the data-frame
    #
    def data_frame_shape(self):
        rows = '{:,}'.format(self.data_frame.shape[0])
        columns = '{:,}'.format(self.data_frame.shape[1])
        return rows, columns

    # Returns the data-frame column headers as a list
    #
    def data_frame_header_check(self):
        col_headers = self.data_frame.columns.tolist()
        return col_headers

    # Returns the maximum and minimum value for a given data-frame column
    #
    def data_frame_min_max_col_value(self, column):
        col_max = self.data_frame[column].max()
        col_min = self.data_frame[column].min()
        return str(col_max), str(col_min)

    # Returns the number of distinct and total values for a given data-frame column
    #
    def data_frame_distinct_values(self, column):
        distinct_values_list = self.data_frame[column].value_counts()
        col_list = self.data_frame[column].count()
        distinct_values = '{:,}'.format(len(distinct_values_list))
        col_count = '{:,}'.format(col_list)
        return distinct_values, col_count

    # Returns the largest and shortest lengths for a given data-frame column
    #
    def data_frame_min_max_lengths(self, column):
        max_len = self.data_frame[column].map(str).apply(len).max()
        min_len = self.data_frame[column].map(str).apply(len).min()
        return str(max_len), str(min_len)
