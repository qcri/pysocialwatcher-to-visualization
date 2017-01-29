# -*- coding: utf-8 -*-
import postProcessDataToVisualization
import os

from subprocess import check_output
ips = check_output(['hostname', '--all-ip-addresses'])
CURRENT_HOSTNAME = check_output(['hostname'])
CURRENT_IP = check_output(['hostname', '--all-ip-addresses'])

# IMPORTANT! Set this variable where the application root data is
APPLICATION_ROOT_DATA_FOLDER = "/var/www/sha/data/"

LOG_FILE_NAME = "logging.txt"
DATA_RAW_OUTPUT_FILE = "data.csv"
JSON_INPUT_FILE = "arabic_health_awareness.json"
FB_CREDENTIALS_FILE = "fb_credentials.csv"
EMAIL_CREDENTIALS_FILE = "email_credentials.csv"
UPDATE_SCRIPT_CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__)) + "/"

COMPRESSED_RAW_FILES_PATH = UPDATE_SCRIPT_CURRENT_FOLDER + "original_responses/"
postProcessDataToVisualization.APPLICATION_CURRENT_DATA_FOLDER = APPLICATION_ROOT_DATA_FOLDER + "current_data/"
postProcessDataToVisualization.HISTORY_FOLDER_NAME = "historic_data/"
postProcessDataToVisualization.HISTORY_FOLDER_PATH = APPLICATION_ROOT_DATA_FOLDER + postProcessDataToVisualization.HISTORY_FOLDER_NAME
postProcessDataToVisualization.HISTORY_MAP_FILE_PATH = postProcessDataToVisualization.HISTORY_FOLDER_PATH + "history_map.csv"
