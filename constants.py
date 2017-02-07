# -*- coding: utf-8 -*-
import postProcessDataToVisualization
import os
from subprocess import check_output
import datetime
ips = check_output(['hostname', '--all-ip-addresses'])
CURRENT_HOSTNAME = check_output(['hostname'])
CURRENT_IP = check_output(['hostname', '--all-ip-addresses']).strip()
NUMBER_OF_REQUESTS = 0
INITIAL_TIME = datetime.datetime.now()

# IMPORTANT! Set this variable where the application root data is
PRODUCTION_SERVER = "10.2.0.108"
if CURRENT_IP == PRODUCTION_SERVER:
    APPLICATION_ROOT_DATA_FOLDER = "/var/www/sha/data/"
else:
    APPLICATION_ROOT_DATA_FOLDER = "../"

LOG_FILE_NAME = "logging.txt"
DATA_RAW_OUTPUT_FILE = "data.csv"
# JSON_INPUT_FILE = "arabic_health_awareness.json"
JSON_INPUT_FILE = "peace_vs_war_collection_input.json"
FB_CREDENTIALS_FILE = "fb_credentials.csv"
EMAIL_CREDENTIALS_FILE = "email_credentials.csv"
UPDATE_SCRIPT_CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__)) + "/"

# Output folders
COMPRESSED_RAW_FILES_PATH = UPDATE_SCRIPT_CURRENT_FOLDER + "original_responses/"
postProcessDataToVisualization.APPLICATION_CURRENT_DATA_FOLDER = APPLICATION_ROOT_DATA_FOLDER + "current_data/"
postProcessDataToVisualization.HISTORY_FOLDER_NAME = "historic_data/"
postProcessDataToVisualization.HISTORY_FOLDER_PATH = APPLICATION_ROOT_DATA_FOLDER + postProcessDataToVisualization.HISTORY_FOLDER_NAME
postProcessDataToVisualization.HISTORY_MAP_FILE_PATH = postProcessDataToVisualization.HISTORY_FOLDER_PATH + "history_map.csv"

#Create folders if dont exist
if not os.path.exists(COMPRESSED_RAW_FILES_PATH):
    print "Creating: " + COMPRESSED_RAW_FILES_PATH
    os.makedirs(COMPRESSED_RAW_FILES_PATH)
if not os.path.exists(postProcessDataToVisualization.APPLICATION_CURRENT_DATA_FOLDER):
    print "Creating: " + postProcessDataToVisualization.APPLICATION_CURRENT_DATA_FOLDER
    os.makedirs(postProcessDataToVisualization.APPLICATION_CURRENT_DATA_FOLDER)
if not os.path.exists(postProcessDataToVisualization.HISTORY_FOLDER_PATH):
    print "Creating: " + postProcessDataToVisualization.HISTORY_FOLDER_PATH
    os.makedirs(postProcessDataToVisualization.HISTORY_FOLDER_PATH)