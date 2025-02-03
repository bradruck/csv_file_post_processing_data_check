# turn_post_processing_manager module
# Module holds the class => PostProcessingManager - manages the Weekly Turn Post-Processing
# Class responsible for overall program management
#
from datetime import datetime, timedelta
import time
import os
import json
import logging

from jira_manager import JiraManager
from csv_manager import CSVManager
from pandas_manager import PandasManager
from zip_manager import ZipManager

today_date = (datetime.now() - timedelta(hours=7)).strftime('%Y%m%d')


class PostProcessingManager(object):
    def __init__(self, config_params):
        self.jira_url = config_params['jira_url']
        self.jira_token = config_params['jira_token']
        self.jira_pars = None
        self.jira_status_parent = config_params['jql_status_parent']
        self.jira_status_child = config_params['jql_status_child']
        self.jira_issuetype = config_params['jql_issuetype']
        self.jira_label = config_params['jql_label']
        self.jira_text = config_params['jql_text']
        self.zfs_path = config_params['zfs_path']
        self.results_json_path = config_params['results_json_path']
        self.results_json_name = config_params['results_json_name']
        self.results_file_name = '{}{}_{}.json'.format(self.results_json_path, self.results_json_name, today_date)
        self.results_dict = {}
        self.parent_tickets = []
        self.child_tickets = []
        self.logger = logging.getLogger(__name__)

    # Manages the process for finding parent tickets, pulls information, populates/runs jira query and returns results
    #
    def parent_ticket_manager(self):
        # Create Jira instance and connect to the Jira Server, if successful, pull desired parent tickets running jql
        try:
            self.jira_pars = JiraManager(self.jira_url, self.jira_token)
        except Exception as e:
            self.logger.info("There was a problem with the Jira server connection - {}".format(e))
        else:
            self.parent_tickets = self.jira_pars.find_parent_tickets(self.jira_issuetype, self.jira_status_parent,
                                                                     self.jira_text)

        # Verifies that tickets were found that match the search criteria and logs count and a list of all tickets
        if self.parent_tickets:
            self.logger.info("{} parent ticket(s) were found that match the criteria.".format(len(self.parent_tickets)))
            self.logger.info("{}\n".format([ticket.key for ticket in self.parent_tickets]))

            # Iterates through list of found parent tickets
            for parent_ticket in self.parent_tickets:
                self.logger.info("\n\t\t\t\t\t\t\t  => Parent Ticket Number: {}".format(parent_ticket))

                # Fetches the relevant parent ticket level information for zip file name creation
                parent_ticket.customer_name = self.jira_pars.parent_information_pull(parent_ticket)
                self.logger.info("\t  => Account/Customer name: {}".format(parent_ticket.customer_name))

                # Pulls desired sub-tasks running jql
                self.child_tickets.clear()
                self.child_tickets = self.jira_pars.find_child_tickets(parent_ticket, self.jira_status_child,
                                                                       self.jira_label)
                if self.child_tickets:
                    self.logger.info("\n{} child ticket(s) were found that match the "
                                     "criteria.".format(len(self.child_tickets)))
                    self.child_ticket_manager(parent_ticket)
                else:
                    self.logger.error("There were no child tickets found with the required criteria to process.")

            # write the overall run results dict to a json file on zfs/operations_limited
            self.json_file_write()

        else:
            self.logger.error("There were no parent tickets found with the required criteria to process.")
        self.jira_pars.kill_session()

    # Manages the process at the child ticket level, data checks, zip file creation, ftp posting
    #
    def child_ticket_manager(self, parent_ticket):
        for child_ticket in self.child_tickets:
            self.logger.info("\n\t  => Child Ticket Number: {}".format(child_ticket))

            # Fetches the relevant child ticket level information then creates zip file name and path
            child_ticket.date_range = self.jira_pars.child_information_pull(child_ticket)
            zip_file_name = '{}_{}'.format(parent_ticket.customer_name, child_ticket.date_range)
            child_ticket_zfs_path = '{}/{}/{}/'.format(self.zfs_path, parent_ticket.key, child_ticket.key)

            # Collects the relevant csv file names and file nicknames - if they exist
            csv_file_names = self.csv_data_fetch(parent_ticket, child_ticket)

            # Performs all the required data checks on the csv files returns a dict with the check info
            checked_files = self.pandas_data_check(csv_file_names)

            # Check that both csv files passed the checks, else by-pass zipping
            if checked_files and checked_files is not None:
                # posts the panda results dictionary for each ticket into an overall run results dictionary
                self.results_dict[child_ticket.key] = checked_files

                # Posts the quality check results as comment on ticket
                self.jira_pars.add_quality_checks_results_comment(child_ticket, checked_files)

                # Creates the zip file on zfs, if successful posts row count comment on ticket, changes 'labels field'
                if self.file_zip(child_ticket_zfs_path, zip_file_name, csv_file_names):
                    self.jira_pars.add_count_comment(child_ticket, zip_file_name, checked_files)
                    self.jira_pars.update_field_value(child_ticket)
            else:
                self.logger.error("The csv files for ticket {} have issues, they failed the data checks, "
                                  "NO zip file was created".format(child_ticket.key))

    # Creates a CSV Manager instance, calls the find_csv_files module and returns csv file names
    #
    def csv_data_fetch(self, parent_ticket, child_ticket):
        csv_data = CSVManager(self.zfs_path)

        # Search zfs/Technology for related csv files, returning a sorted list of lists with file nicknames and names
        try:
            csv_file_list = csv_data.find_csv_files(parent_ticket, child_ticket)
        except Exception as e:
            self.logger.error("There was a problem with the csv files for ticket: {} - {}".format(child_ticket.key, e))
        else:
            return csv_data.sort_file_list(csv_file_list)

    # Calls all the data checks utilizing Pandas data-frame creation and Pandas functions
    #
    def pandas_data_check(self, file_names):
        ticket_quality_results = {}  # dictionary to hold pandas data-check results dictionaries for both files

        # Run the checks file by file
        for file_name in file_names:
            pandas_data = {}  # dictionary to hold pandas data-check results for each file

            # Create a data frame instance and check for any missing values
            pandas_data_frame = PandasManager()

            # Check that the data_frame was created successfully and check for any null values
            if pandas_data_frame.data_frame_load(file_name[1]) is not None:
                # Assign the file name to dictionary
                pandas_data['file name'] = file_name[1].split('/')[-1]

                # Find the data dimensions, rows and columns
                try:
                    row_count, column_count = pandas_data_frame.data_frame_shape()
                except Exception as e:
                    self.logger.error("The row and column counts failed." + " - {}".format(e))
                    return None
                else:
                    pandas_data['file rows'] = row_count
                    pandas_data['file columns'] = column_count

                # Find the column headers
                try:
                    col_headers = pandas_data_frame.data_frame_header_check()
                except Exception as e:
                    self.logger.error("The row and column header check failed. - {}".format(e))
                    return None
                else:
                    pandas_data['column headers'] = col_headers

                # Run checks column by column
                for column in col_headers:
                    # Find maximum and minimum values in column
                    if column in ['transactionDateTime', 'units']:
                        try:
                            col_max, col_min = pandas_data_frame.data_frame_min_max_col_value(str(column))
                        except Exception as e:
                            self.logger.error("The max and min values check failed. - {}".format(e))
                            return None
                        else:
                            pandas_data[column + ' max value'] = col_max
                            pandas_data[column + ' min value'] = col_min

                    # Find maximum and minimum lengths in column
                    if column in ['xid', 'txn_id', 'upc']:
                        try:
                            max_len, min_len = pandas_data_frame.data_frame_min_max_lengths(column)
                        except Exception as e:
                            self.logger.error("The max and min lengths check failed. - ".format(e))
                            return None
                        else:
                            pandas_data[column + ' max length'] = max_len
                            pandas_data[column + ' min length'] = min_len

                    # Find total number and distinct number of values in column
                    if column == 'txn_id' and file_name[0] == 'id':
                        try:
                            [distinct_values, col_count] = pandas_data_frame.data_frame_distinct_values(column)
                        except Exception as e:
                            self.logger.error("The distinct and total values check failed. - ".format(e))
                            return None
                        else:
                            pandas_data[column + ' distinct values'] = distinct_values
                            pandas_data[column + ' count'] = col_count

            else:
                self.logger.warning("Pandas data frame load issue => {}".format(file_name[1]),
                                    "\n\t check that the csv file exists, if so, check for the proper delimiters - '|'",
                                    "\n\n =>  Moving on to next csv file")
                return None

            # Add the dictionary pandas_data as a value in a ticket level results dictionary with key value of file_type
            ticket_quality_results[file_name[0]] = pandas_data
        return ticket_quality_results

    # Creates a Zip Manager instance, calls the create_zip_file module and returns the full zip file path and name
    #
    def file_zip(self, child_ticket_zfs_path, zip_file_name, csv_file_names):
        # ***line below creates zip files in local directory instead of on zfs, comment out for production***
        #child_ticket_zfs_path = '/Users/bradley.ruck/CI_Projects/prod_versions/Data_Enablement_Turn_Post_Processing/'
        zipper = ZipManager(child_ticket_zfs_path, zip_file_name)

        # Create zip file from the two csv files in zfs/Technology directory
        try:
            zipper.create_zip_file(csv_file_names)
        except Exception as e:
            self.logger.error("There was a problem zipping the file: {} - {}".format(zip_file_name, e))
            return False
        else:
            self.logger.info("The zip file {} has been created".format(zip_file_name))
            return True

    # Writes the run data to a json file as a history repository and potential further processing
    #
    def json_file_write(self):
        try:
            # create json file for results repository, to be stored on zfs1/operations_limited drive
            with open(self.results_file_name, 'w') as fp:
                json.dump(self.results_dict, fp, indent=4)
        except Exception as e:
            self.logger.warning("There was a problem creating the json data file or posting it to "
                                "/zfs1/operations_limitted => {}".format(e))
        else:
            self.logger.info("There results have been posted to: {}".format(self.results_file_name))

    # Checks the log directory for all files and removes those after a specified number of days
    #
    def purge_files(self, purge_days, purge_dir):
        try:
            self.logger.info("\n\t\tRemove {} days old files from the {} directory".format(purge_days, purge_dir))
            now = time.time()
            for file_purge in os.listdir(purge_dir):
                f_obs_path = os.path.join(purge_dir, file_purge)
                if os.stat(f_obs_path).st_mtime < now - int(purge_days) * 86400:
                    time_stamp = time.strptime(time.strftime('%Y-%m-%d %H:%M:%S',
                                                             time.localtime(os.stat(f_obs_path).st_mtime)),
                                               "%Y-%m-%d %H:%M:%S")
                    self.logger.info("Removing File [{}] with timestamp [{}]".format(f_obs_path, time_stamp))
                    os.remove(f_obs_path)

        except Exception as e:
            self.logger.warning("{}".format(e))
