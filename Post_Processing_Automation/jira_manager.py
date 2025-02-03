# jira_manager module
# Module holds the class => JiraManager - manages JIRA ticket interface
# Class responsible for all JIRA related interactions including ticket searching, data pull, file attaching, comment
# posting and field updating.
#
from jira import JIRA
from datetime import datetime, timedelta
import re
import logging


class JiraManager(object):
    def __init__(self, url, jira_token):
        self.parent_tickets = []
        self.child_tickets = []
        self.jira = JIRA(url, basic_auth=jira_token)
        self.date_range = ""
        self.file_name = ""
        self.advert_field_name = ""
        self.advertiser_name = ""
        self.logger = logging.getLogger(__name__)
        self.today_date = (datetime.now() - timedelta(hours=6)).strftime('%m/%d/%Y')
        self.comment_alert = 'zack.batt'
        self.zip_file_created_alert = 'The zip file has been created and the file counts are below:'
        self.quality_results_alert = 'These are the results of the quality checks: '

    # Searches Jira for all tickets that match the parent ticket query criteria
    #
    def find_parent_tickets(self, issuetype, status, text):
        # Query to find qualified Jira Tickets, includes matches for text: including 'Turn' but excluding 'Test'
        jql_query = "project IN (CAM) AND issuetype = " + issuetype + " AND status in " + status + " AND summary ~ " \
                    + text
        self.parent_tickets = self.jira.search_issues(jql_query)
        return self.parent_tickets

    # Retrieves the required data from parent ticket to populate email
    #
    def parent_information_pull(self, ticket):
        ticket = self.jira.issue(ticket.key)
        # Selects the final split value in the 'Summary' field and strips it of beginning and ending whitespace
        self.advert_field_name = ticket.fields.summary.split('-')[-1].strip()
        # Creates a name list split along whitespace and also splits if CamelHump notation exists
        split_name = re.sub('(?!^)([A-Z][a-z]+)', r' \1', self.advert_field_name).split()
        # Remove '_' character from words in list if exists
        split_name = ' '.join(split_name).replace('_', '').split()
        self.advertiser_name = self.normalize_name(split_name)
        return self.advertiser_name

    # Searches Jira for tickets that are sub-tasks of the list of parent tickets and require an email to be sent
    #
    def find_child_tickets(self, ticket, status, label):
        jql_query = "parent in (" + ticket.key + ") AND status = " + status + " AND labels = " + label
        self.child_tickets.extend(self.jira.search_issues(jql_query))
        return self.child_tickets

    # Retrieves the required data from child ticket to populate email
    #
    def child_information_pull(self, ticket):
        ticket = self.jira.issue(ticket.key)
        start_date = datetime.strptime(ticket.fields.customfield_10431, "%Y-%m-%d").strftime("%Y-%m-%d")
        end_date = datetime.strptime(ticket.fields.customfield_10418, "%Y-%m-%d").strftime("%Y-%m-%d")
        self.date_range = '{}_{}'.format(start_date, end_date)
        return self.date_range

    # Add a comment on ticket with zip file creation alert and csv file counts
    #
    def add_count_comment(self, ticket, zip_file_name, quality_checks):
        ticket = self.jira.issue(ticket.key)
        reporter = ticket.fields.reporter.key
        message = """[~{attention}]
                     {zip_alert}
                     
                     *{zip_file_name}.zip*
                     
                     ||File Name||Row Count||
                     |{first_file}|Q = {first_rows}|
                     |{second_file}|Q = {second_rows}|
                     """.format(reporter, attention=self.comment_alert,
                                zip_alert=self.zip_file_created_alert,
                                first_file=quality_checks.get('id', {}).get('file name'),
                                first_rows=quality_checks.get('id', {}).get('file rows'),
                                second_file=quality_checks.get('upc', {}).get('file name'),
                                second_rows=quality_checks.get('upc', {}).get('file rows'),
                                zip_file_name=zip_file_name)

        self.jira.add_comment(issue=ticket, body=message)
        self.logger.info("The zip file creation alert and the cvs file line counts have been added as a comment to "
                         "Jira Ticket: {}".format(ticket.key))

    # Add a comment to ticket with the quality checks results
    #
    def add_quality_checks_results_comment(self, ticket, quality_checks):
        ticket = self.jira.issue(ticket.key)
        reporter = ticket.fields.reporter.key
        message = """[~{attention}]
                     {quality_results_alert}
        
                     *{first_file_name}*
                     ||Column Header||Quality Check||Result||
                     |{first1_header}||Max Length||{max_lengths1}|
                     | ||Min Length||{min_lengths1}|
                     |{second1_header}||Max Length||{max_lengths2}|
                     | ||Min Length||{min_lengths2}|
                     |{second1_header}||Distinct Values||{dist_values}|
                     | ||Total Values||{total_values}|
                     
                     *{second_file_name}*
                     ||Column Header||Quality Check||Result||
                     |{first2_header}||Max Length||{max_lengths3}|
                     | ||Min Length||{min_lengths3}|
                     |{second2_header}||Max Date||{max_dates}|
                     | ||Min Date||{min_dates}|
                     |{third2_header}||Max Length||{max_lengths4}|
                     | ||Min Length||{min_lengths4}|
                     |{fourth2_header}||Max Value||{max_values}|
                     | ||Min Value||{min_values}|
                     """.format(reporter, attention=self.comment_alert,
                                quality_results_alert=self.quality_results_alert,
                                first_file_name=quality_checks.get('id', {}).get('file name'),
                                first1_header=quality_checks.get('id', {}).get('column headers')[0],
                                max_lengths1=quality_checks.get('id', {}).get('xid max length'),
                                min_lengths1=quality_checks.get('id', {}).get('xid min length'),
                                second1_header=quality_checks.get('id', {}).get('column headers')[1],
                                max_lengths2=quality_checks.get('id', {}).get('txn_id max length'),
                                min_lengths2=quality_checks.get('id', {}).get('txn_id min length'),
                                dist_values=quality_checks.get('id', {}).get('txn_id distinct values'),
                                total_values=quality_checks.get('id', {}).get('txn_id count'),
                                second_file_name=quality_checks.get('upc', {}).get('file name'),
                                first2_header=quality_checks.get('upc', {}).get('column headers')[0],
                                max_lengths3=quality_checks.get('upc', {}).get('txn_id max length'),
                                min_lengths3=quality_checks.get('upc', {}).get('txn_id min length'),
                                second2_header=quality_checks.get('upc', {}).get('column headers')[1],
                                max_dates=quality_checks.get('upc', {}).get('transactionDateTime max value'),
                                min_dates=quality_checks.get('upc', {}).get('transactionDateTime min value'),
                                third2_header=quality_checks.get('upc', {}).get('column headers')[2],
                                max_lengths4=quality_checks.get('upc', {}).get('upc max length'),
                                min_lengths4=quality_checks.get('upc', {}).get('upc min length'),
                                fourth2_header=quality_checks.get('upc', {}).get('column headers')[3],
                                max_values=quality_checks.get('upc', {}).get('units max value'),
                                min_values=quality_checks.get('upc', {}).get('units min value')
                                )
        self.jira.add_comment(issue=ticket, body=message)
        self.logger.info("The quality checks results have been added as a comment to "
                         "Jira Ticket: {}".format(ticket.key))

    # Change the field 'labels' in the child ticket to the value 'CVSFiles_Counted' to omit from future search results
    #
    @staticmethod
    def update_field_value(ticket):
        ticket.fields.labels.append(u'ZipFile_Created')
        ticket.update(fields={'labels': ticket.fields.labels})

    # Applies rules to normalize the Advertiser names into Data Enablement accepted file-naming convention
    #
    @staticmethod
    def normalize_name(split_name):
        # Rules for Del Monte, can have either 3 or 4 names
        if split_name[0] == 'Del':
            if len(split_name) > 3:
                d = [split_name[0] + split_name[1], split_name[2] + split_name[3]]
                advertiser_name = "_".join(d)
            else:
                d = [split_name[0] + split_name[1], split_name[2]]
                advertiser_name = "_".join(d)
        # Rules for Cytosport, can have 4 names
        elif split_name[0] == 'Cytosport':
            d = [split_name[0], split_name[1] + split_name[2] + split_name[3]]
            advertiser_name = "_".join(d)
        # Rules for Colgate and Blackbox, can have either 2 or 3 names
        else:
            if len(split_name) > 2:
                d = [split_name[0], split_name[1] + split_name[2]]
                advertiser_name = "_".join(d)
            else:
                advertiser_name = "_".join(split_name)

        return advertiser_name

    # Ends the current JIRA session
    #
    def kill_session(self):
        self.jira.kill_session()
