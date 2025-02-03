# csv_manager module
# Module holds the class => CVSManager - manages the CVS File Interface
# Class responsible for the CVS file name search, file type assignment and returns list of file names with file types
#
from glob import glob


class CSVManager(object):
    def __init__(self, zfs_path):
        self.zfs_path = zfs_path
        self.file_type = ""
        self.path = ""

    # Locate and format the 2 csv file names, returns a list of lists with the full file path+name and type for each
    #
    def find_csv_files(self, parent_ticket, child_ticket):
        self.path = '{}{}/{}/'.format(self.zfs_path, parent_ticket.key, child_ticket.key)
        file_names = self.get_file_names('{}*.csv'.format(self.path))
        return file_names

    # sort list so id file is first and upc file is second, and add a file type to each, returning a list of lists
    #
    def sort_file_list(self, file_names):
        sorted_file_names = []
        # sort file names to ensure id first and upc second
        file_names.sort()
        # create a list of lists that includes file type and file name
        for file_name in file_names:
            file_type = self.file_name_type(file_name)
            '{}/{}'.format(self.path, file_name)
            sorted_file_names.append([file_type, file_name])
        return sorted_file_names

    # Creates and returns a file nickname, either upc or id for the corresponding file type
    #
    def file_name_type(self, file_name):
        file_stub = file_name.split('_')[-1]
        self.file_type = file_stub.split('.')[0]
        return self.file_type

    # Search specified zfs folder for what should be 2 files, get names and return as a list of file names
    #
    @staticmethod
    def get_file_names(path):
        both_files = []
        for file_name in glob(path):
            both_files.append(file_name)
        return both_files
