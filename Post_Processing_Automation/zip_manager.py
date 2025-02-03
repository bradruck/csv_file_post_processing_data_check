# zip_manager module
# Module holds the class => ZipManager - manages zip file creation interface
# Class responsible for all zip file related interactions including creation on zfs/Technology drive
#
from zipfile import ZipFile
import os


class ZipManager(object):
    def __init__(self, child_ticket_zfs_path, zip_file_name):
        self.zip_file_name = '{}{}.zip'.format(child_ticket_zfs_path, zip_file_name)

    # Creates a zip file in the zfs/Technology directory where the csv files are located
    #
    def create_zip_file(self, file_names):
        with ZipFile(self.zip_file_name, 'w') as zipped:
            for file in file_names:
                zipped.write(file[1], os.path.basename(file[1]))
