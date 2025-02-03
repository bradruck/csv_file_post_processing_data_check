# Data Enablement - Data Append, TURN-Post Processing

# Description -
# The Data Enablement Turn Post Processor is an automation for the weekly requirement of assessing the Tigershark
# created csv files for quality and quantity of data prior to customer shipment/delivery.
# The automation is scheduled to run via an ActiveBatch trigger launched via a text-file drop into a designated folder
# located at /zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Trigger_PP/.
# The automation begins by conducting a JIRA search for sub-task (child) tickets that are created from parent TURN
# Tickets. Both parent and child tickets are mined for information that allows the automation to find the location of
# the csv files in the zfs1/Technology/Tigershark/data_license directory. After the csv file locations have been
# identified, the contents of each file are loaded into a Pandas data-frame for analysis.  Row and column counts, column
# header names are returned. Data lengths and values are analyze for maximum and minimum amounts and the data is
# evaluated for proper delimiter (pipe in this case) and a lack of missing information. The quality check results are
# then posted as a comment to the Jira ticket and a separate comment is posted stating the row counts for each csv file.
# The run results in total are copied into a json file on the operations_limited drive on zfs1 inside the Logs_PP folder
# Finally, upon successful data checks, the two csv files are zipped together and placed into the same folder as the
# original csv files. The automation intentionally ceases here to allow an eyes-on review of results prior to shipment
# (loading) of the end product to the customer ftp site.
#
# Application Information -
# Required modules:     main.py,
#                       turn_post_processing_manager.py,
#                       jira_manager.py,
#                       csv_manager.py,
#                       pandas_manager.py,
#                       zip_manager.py,
#                       config.ini
# Deployed Location:    //prd-use1a-pr-34-ci-operations-01/home/bradley.ruck/Projects/data_enablement_pp/
# ActiveBatch Trigger:  //aws-p-nv-ci02/prd-09-abjs-01 (V11)/'Jobs, Folders & Plans'/Operations/Report/DE_PP/
# Source Code:          https://gitlab.oracledatacloud.com/odc-operations/DE_Turn_PP/
# LogFile Location:     //zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Logs_PP/
# ResultsFile Location: //zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Logs_PP/Results/
#
# Contact Information -
# Primary Users:    Data Enablement
# Lead Customer:    Zack Batt (zack.batt@oracle.com)
# Lead Developer:   Bradley Ruck (bradley.ruck@oracle.com)
# Date Launched:    May 2018
# Date Updated:     January 2019

# main module
# Responsible for reading in the basic configurations settings, creating the log file, and creating and launching
# the Data Enablement Post Processing Manager (TPPM), finally it launches the purge_files method to remove log files
# that are older than a prescribed retention period
#
import configparser
from datetime import datetime, timedelta
import os
import logging

from VaultClient3 import VaultClient3 as VaultClient
from turn_post_processing_manager import PostProcessingManager


# Define a console logger for development purposes
#
def console_logger():
    # define Handler that writes DEBUG or higher messages to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a simple format for console use
    formatter = logging.Formatter('%(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s')
    console.setFormatter(formatter)
    # add the Handler to the root logger
    logging.getLogger('').addHandler(console)


def main(con_opt='n'):
    today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y%m%d-%H%M%S')

    # Get config files
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Vault Client Object
    VC_Obj = VaultClient("prod")
    pd = VC_Obj.VaultSecret('jira', str(config.get('Jira', 'authorization')))

    # Create a dictionary of configuration parameters
    config_params = {
        "jira_url":             config.get('Jira', 'url'),
        "jira_token":           tuple([config.get('Jira', 'authorization'), pd]),
        "jql_status_parent":    config.get('Jira', 'status_parent'),
        "jql_status_child":     config.get('Jira', 'status_child'),
        "jql_issuetype":        config.get('Jira', 'issuetype'),
        "jql_label":            config.get('Jira', 'label'),
        "jql_text":             config.get('Jira', 'text'),
        "zfs_path":             config.get('cvsFile', 'path'),
        "results_json_path":    config.get('ResultsFile', 'path'),
        "results_json_name":    config.get('Project Details', 'app_name')
    }

    # Logfile path to point to the Operations_limited drive on zfs
    purge_days = config.get('LogFile', 'retention_days')
    log_file_path = config.get('LogFile', 'path')
    logfile_name = '{}{}_{}.log'.format(log_file_path, config.get('Project Details', 'app_name'), today_date)

    # Check to see if log file already exits for the day to avoid duplicate execution
    if not os.path.isfile(logfile_name):
        logging.basicConfig(filename=logfile_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')

        logger = logging.getLogger(__name__)

        # this is only enacted if main.py is run as the executable
        if con_opt and con_opt in ['y', 'Y']:
            console_logger()

        logger.info("Process Start - Weekly Turn Post-Processing for Data Enablement - {}\n".format(today_date))

        # Create TPPM object and launch the Post Processor
        de_turn_pp = PostProcessingManager(config_params)
        de_turn_pp.parent_ticket_manager()

        # Search logfile directory for old log files to purge
        de_turn_pp.purge_files(purge_days, log_file_path)


if __name__ == '__main__':
    # prompt user for use of console logging -> for use in development not production
    ans = input("\nWould you like to enable a console logger for this run?\n Please enter y or n:\t")
    print()
    main(ans)
