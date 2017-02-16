# -*- coding: utf-8 -*-
from pysocialwatcher import watcherAPI
import os
import yagmail
import keyrings.alt
import logging
import sys
import traceback
import time
from constants import *


send_error_to = [
    "maraujo@hbku.edu.qa",
#    "msthing@gmail.com",
#    "iweber@hbku.edu.qa"
]

send_success_to = [
    "maraujo@hbku.edu.qa",
    # "msthing@gmail.com",
    # "iweber@hbku.edu.qa"

]

def get_error_message():
    traceback_msg = traceback.format_exc()
    message = "Update Arabic Awareness Error\n"
    message += "HOSTNAME: " + CURRENT_HOSTNAME + "\n"
    message += "HOST IP: " + CURRENT_IP + "\n"
    message += "Update Script Path: " + UPDATE_SCRIPT_CURRENT_FOLDER + "\n"
    message += "-----------------------------------------------------------\n"
    message += "LOG MESSAGE FILE:\n"
    message += LOG_FILE_NAME + "\n"
    message += "-----------------------------------------------------------\n"
    message += "TRACEBACK MESSAGE:\n"
    message += traceback_msg
    return message

def get_success_message(NUMBER_OF_REQUESTS):
    FINISH_TIME = datetime.datetime.now()
    DURATIION = FINISH_TIME - INITIAL_TIME
    message = "SUCCESS:\n"
    message += "HOSTNAME: " + CURRENT_HOSTNAME + "\n"
    message += "HOST IP: " + CURRENT_IP + "\n"
    message += "-----------------------------------------------------------\n"
    message += "SUMMARY:\n"
    message += "Initial Time: " + str(INITIAL_TIME) + "\n"
    message += "End Time: " + str(FINISH_TIME) + "\n"
    message += "Duration: " + str(DURATIION) + "\n"
    message += "Number of Facebook Requests: " + str(NUMBER_OF_REQUESTS) + "\n"
    message += "Log File:" + LOG_FILE_NAME + "\n"
    message += "-----------------------------------------------------------\n"
    message += "REFERENCE PATHS:\n"
    message += "Update Script Path: " + UPDATE_SCRIPT_CURRENT_FOLDER + "\n"
    message += "Application History Map File Path: " + postProcessDataToVisualization.HISTORY_MAP_FILE_PATH + "\n"
    message += "Application Current Data Folder Path: " + postProcessDataToVisualization.APPLICATION_CURRENT_DATA_FOLDER + "\n"
    message += "Application History Data Folder Path: " + postProcessDataToVisualization.HISTORY_FOLDER_PATH + "\n"
    message += "Raw Facebook Requests Data Path: " + COMPRESSED_RAW_FILES_PATH + "\n"
    return message

def send_email_success(NUMBER_OF_REQUESTS):
    email_credentials = open(EMAIL_CREDENTIALS_FILE, "r").read().strip().split(",")
    yagmail.SMTP(email_credentials[0], email_credentials[1]).send(to=send_success_to, subject="Success: Arabic Awareness Data Updated", contents= get_success_message(NUMBER_OF_REQUESTS))
    print "Success email sent."

def send_email_error(subject):
    email_credentials = open(EMAIL_CREDENTIALS_FILE, "r").read().strip().split(",")
    email_body = get_error_message()
    yagmail.SMTP(email_credentials[0], email_credentials[1]).send(to=send_error_to, subject=subject, contents=email_body)
    print "Error email sent."


def init_socialWatcher_and_check_credentials():
    try:
        dataCollector = watcherAPI()
        dataCollector.load_credentials_file(FB_CREDENTIALS_FILE)
        dataCollector.check_tokens_account_valid()
        return dataCollector
    except Exception as Error:
        send_email_error("Error: Credentials invalid")
        raise Error


def save_original_data():
    logging.info("Saving compressed original data")
    dataframe = postProcessDataToVisualization.pd.read_csv(DATA_RAW_OUTPUT_FILE)
    dataframe["timestamp"] = time.time()
    raw_file_name = "data" + postProcessDataToVisualization.CURRENT_DATE_SERIAL + "-" + postProcessDataToVisualization.UNIQUE_TIME_ID + ".gz"
    dataframe.to_csv(COMPRESSED_RAW_FILES_PATH + raw_file_name, compression="gzip")


def run_data_collection(dataCollector):
    try:
        dataframe = dataCollector.run_data_collection(JSON_INPUT_FILE)
        dataframe.to_csv(DATA_RAW_OUTPUT_FILE)
        return dataframe
    except Exception as Error:
        send_email_error("Error: Collecting Data")
        raise Error


def post_process_data():
    try:
        # save_original_data()
        postProcessData = postProcessDataToVisualization.PostProcessVisualizationData("data.csv")
        postProcessData.process_data()
    except Exception as Error:
        # send_email_error("Error: Post Processing")
        raise Error

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    logger = logging.getLogger()
    logger.addHandler(logging.FileHandler(LOG_FILE_NAME, 'w'))
    # dataCollector = init_socialWatcher_and_check_credentials()
    # dataframe = run_data_collection(dataCollector)
    post_process_data()
    # send_email_success(len(dataframe))
    # os.system("rm dataframe_*.csv")
    # os.system("rm collect_*.csv")
