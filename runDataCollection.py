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
    "msthing@gmail.com",
    "iweber@hbku.edu.qa"
]

send_success_to = [
    "maraujo@hbku.edu.qa",
    "msthing@gmail.com",
    "iweber@hbku.edu.qa"
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
    try:
        email_credentials = open(EMAIL_CREDENTIALS_FILE, "r").read().strip().split(",")
        yagmail.SMTP(email_credentials[0], email_credentials[1]).send(to=send_success_to, subject="Success: Arabic Awareness Data Updated", contents= get_success_message(NUMBER_OF_REQUESTS))
        print "Success email sent."
    except:
        print "Warning: Could not send an success email."

def send_email_error(subject):
    try:
        email_credentials = open(EMAIL_CREDENTIALS_FILE, "r").read().strip().split(",")
        email_body = get_error_message()
        yagmail.SMTP(email_credentials[0], email_credentials[1]).send(to=send_error_to, subject=subject, contents=email_body)
        print "Error email sent."
    except:
        print "ERROR: Could not send an ERROR email. You should check what happened."


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
    raw_file_name = JSON_INPUT_FILE.split(".")[0] + "-" + postProcessDataToVisualization.CURRENT_DATE_SERIAL + "-" + postProcessDataToVisualization.UNIQUE_TIME_ID + ".gz"
    dataframe.to_csv(COMPRESSED_RAW_FILES_PATH + raw_file_name, compression="gzip")


def run_data_collection():
    try:
        dataCollector = init_socialWatcher_and_check_credentials()
        dataframe = dataCollector.run_data_collection(JSON_INPUT_FILE)
        dataframe.to_csv(DATA_RAW_OUTPUT_FILE)
        return dataframe
    except Exception as Error:
        send_email_error("Error: Collecting Data")
        raise Error


def post_process_data():
    try:
        save_original_data()
        postProcessData = postProcessDataToVisualization.PostProcessVisualizationData(DATA_RAW_OUTPUT_FILE)
        postProcessData.process_data()
    except OSError as Error:
        # If file already exist
        if Error.errno == 17:
            return
        send_email_error("Error: Post Processing: " + Error.message)
        raise Error
    except Exception as Error:
        send_email_error("Error: Post Processing: " + Error.message)
        raise Error

def continue_collection(collection_file):
    try:
        dataCollector = init_socialWatcher_and_check_credentials()
        dataframe = dataCollector.load_data_and_continue_collection(collection_file)
        dataframe.to_csv(DATA_RAW_OUTPUT_FILE)
        return dataframe
    except Exception as Error:
        send_email_error("Error: Collecting Data")
        raise Error

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='You should configure the "data_folder" output, the "json_input" for the data collection input, "raw_output" for the data collection output .csv file')
    parser.add_argument('--data_folder')
    parser.add_argument('--json_input')
    parser.add_argument('--raw_output')
    parser.add_argument('--continue_collection')
    parser.add_argument('--email', action="store_true")
    args = parser.parse_args()
    if not args.email:
        send_error_to = [send_error_to[0]]
        send_success_to = [send_success_to[0]]
        
    if args.continue_collection:
        dataframe = continue_collection(args.continue_collection)
        send_email_success(len(dataframe))
        sys.exit(0)
    if args.data_folder != None:
        DATA_RAW_OUTPUT_FILE = args.data_folder
    if args.json_input != None:
        JSON_INPUT_FILE = args.json_input
    if args.raw_output != None:
        APPLICATION_ROOT_DATA_FOLDER = args.raw_output




    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    logger = logging.getLogger()
    logger.addHandler(logging.FileHandler(LOG_FILE_NAME, 'w'))
    dataframe = run_data_collection()
    post_process_data()
    send_email_success(len(dataframe))
    os.system("rm dataframe_*.csv")
    os.system("rm collect_*.csv")
