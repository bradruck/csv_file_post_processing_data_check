**Description -**

The Data Enablement Turn Post Processor is an automation for the weekly requirement of assessing the Tigershark
created csv files for quality and quantity of data prior to customer shipment/delivery.
The automation is scheduled to run via an ActiveBatch trigger launched via a text-file drop into a designated folder
located at /zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Trigger_PP/.
The automation begins by conducting a JIRA search for sub-task (child) tickets that are created from parent TURN
Tickets. Both parent and child tickets are mined for information that allows the automation to find the location of
the csv files in the zfs1/Technology/Tigershark/data_license directory. After the csv file locations have been
identified, the contents of each file are loaded into a Pandas data-frame for analysis.  Row and column counts, column
header names are returned. Data lengths and values are analyze for maximum and minimum amounts and the data is evaluated
for proper delimiter (pipe in this case) and a lack of missing information. The quality check results are then posted as
a comment to the Jira ticket and a separate comment is posted stating the row counts for each csv file.  Finally, upon
successful data checks, the two csv files are zipped together and placed into the same folder as the original csv
files. The automation intentionally ceases here to allow an eyes-on review of results prior to shipment (loading)
of the end product to the ftp site for customer collection.

**Application Information -**

Required modules: <ul>
                  <li>main.py,
                  <li>turn_post_processing_manager.py,
                  <li>jira_manager.py,
                  <li>csv_manager.py,
                  <li>pandas_manager.py,
                  <li>zip_manager.py,
                  <li>config.ini
                  </ul>

Location:         <ul>
                  <li>Scheduled on ActiveBatch: //onlinemodelingdev/Jobs, Folders & Plans/Report/DE_PP/, with a file trigger
                  <li>Deployed:  //prd-use1a-pr-34-ci-operations-01/home/bradley.ruck/Projects/data_enablement_pp/
                  </ul>

Source Code:      <ul>
                  <li>https://gitlab.oracledatacloud.com/odc-operations/DE_Turn_PP/
                  </ul>

LogFile Location: <ul>
                  <li>//zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Logs_PP/
                  </ul>

**Contact Information -**

Primary Users:    <ul>
                  <li>Data Enablement
                  </ul>

Lead Customer:    <ul>
                  <li>
                  </ul>

Lead Developer:   <ul>
                  <li>Bradley Ruck (bradley.ruck@oracle.com)
                  </ul>

Date Launched:    <ul>
                  <li>May, 2018
                  </ul>
