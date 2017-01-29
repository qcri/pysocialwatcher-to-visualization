import argparse
import pandas as pd
import itertools
import time
import sys
import zipfile
import os
import ast
import datetime
import shutil
import logging

NULL_VALUE = "NOTSELECTED"
ALL_VALUE = "ALL"
CURRENT_DATE_SERIAL = str(datetime.date.today())
UNIQUE_TIME_ID = str(time.time()).split(".")[0]
MIN_AGE = "min_age"
MAX_AGE = "max_age"
SCHOLARITY = "scholarity"
INTEREST = "interests"
TARGETING = "targeting"
COUNTRY_CODE = "country_code"
TOPIC = "topic"
GEO_LOCATIONS = "geo_locations"
GENDER = "gender"
LANGUAGES = "languages"
AUDIENCE = "audience"
AGE_RANGE = "age_range"
CITIZENSHIP = "citizenship"
RESPONSE = "response"
APPLICATION_CURRENT_DATA_FOLDER = "../current_data/"
HISTORY_FOLDER_NAME = "historic_data/"
HISTORY_FOLDER_PATH = "../" + HISTORY_FOLDER_NAME
HISTORY_MAP_FILE_PATH = HISTORY_FOLDER_PATH + "history_map.csv"

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def unstrict_literal_eval(string):
    try:
        value = ast.literal_eval(string)
        return value
    except ValueError:
        return string
    except SyntaxError:
        return string


def load_dataframe_from_file(file_path):
    dataframe = pd.DataFrame.from_csv(file_path)
    return dataframe.applymap(unstrict_literal_eval)

def get_topic_from_row(row):
    if not pd.isnull(row.interests):
        return row[INTEREST]["name"]

def get_citizenship_from_row(row):
    return row[CITIZENSHIP]["name"]

def get_country_code_from_row(row):
    return row[GEO_LOCATIONS]["values"][0]

def get_scholarity_from_row(row):
    return row[SCHOLARITY]["name"]



class PostProcessVisualizationData:
    expat_counter = 0

    @staticmethod
    def get_age_range(row):
        age_range = row[AGE_RANGE]
        min_age = age_range["min"] if age_range.has_key("min") else NULL_VALUE
        max_age = age_range["max"] if age_range.has_key("max") else NULL_VALUE
        print min_age, max_age
        if max_age == NULL_VALUE and min_age == 18:
            return "{}+".format(int(float(min_age)))
        if max_age == NULL_VALUE and min_age == 45:
            return "{}+".format(int(float(min_age)))
        else:
            return "{}-{}".format(int(float(min_age)), int(float(max_age)))

    def get_expat_row(self, row_with_all, rows_with_locals):
        logging.info("Get Expat Row: {}".format(self.expat_counter))
        self.expat_counter += 1
        expat_row = row_with_all.copy()
        expat_row["citizenship"] = "Expats"

        same_necessary_columns = [SCHOLARITY, INTEREST, GEO_LOCATIONS, GENDER, LANGUAGES, MIN_AGE, MAX_AGE]
        for column in same_necessary_columns:
            rows_with_locals = rows_with_locals[rows_with_locals[column] == row_with_all[column]]
        if len(rows_with_locals) != 1:
            import ipdb;ipdb.set_trace()
            raise Exception("Should have 1 and only 1 local_row that matchs")
        row_with_local = rows_with_locals.iloc[0]
        expat_row[AUDIENCE] = row_with_all[AUDIENCE] - row_with_local[AUDIENCE]
        if expat_row[AUDIENCE] < 0:
            expat_row[AUDIENCE] = 0
        return expat_row


    def replace_specific_key_value(self, column, old_value, new_value):
        logging.info("Replacing : {}:{} por {}:{}".format(column,old_value,column,new_value))
        self.data.loc[self.data[column] == old_value, column] = new_value
        print len(self.data)

    def generate_age_range_column(self):
        logging.info("Inserting Age Range...")
        self.data[AGE_RANGE] = self.data.apply(lambda row: self.get_age_range(row), axis=1)
        print len(self.data)

    def check_not_permitted_empty_values(self):
        logging.info("Checking Not Permitted Empty Values...")
        no_nan_columns = [GEO_LOCATIONS, GENDER, "audience"]
        for column in no_nan_columns:
            if self.data[column].isnull().values.any():
                raise Exception("Not Allowed Null at:" + column)
        print "Number of Lines:", len(self.data)

    def replace_null_values(self):
        logging.info("Replace Null Values...")
        null_values_means_all_columns = [CITIZENSHIP, SCHOLARITY]
        #Gender Case
        self.data[GENDER] = self.data[GENDER].replace(0, "ALL")
        # Replace necessary NULL's to ALL
        for column in null_values_means_all_columns:
            self.data[column] = self.data[column].fillna(ALL_VALUE)
        self.data = self.data.fillna(NULL_VALUE)
        print len(self.data)

    def delete_specific_key_value(self, key, value):
        logging.info("Deleting specific key valye: {}:{}".format(key,value))
        self.data = self.data[self.data[key] != value]
        print len(self.data)

    def rename_column(self,old_name,new_name):
        logging.info("Renaming: {}->{}".format(old_name, new_name))
        columns_names = {old_name : new_name}
        self.data = self.data.rename(columns=columns_names)
        print len(self.data)

    def insert_expats_native_rows(self):
        logging.info("Adding Not Native Rows")
        rows_with_all = self.data[self.data["citizenship"] == ALL_VALUE]
        rows_with_locals = self.data[self.data["citizenship"] == "Locals"]
        rows_with_expats = rows_with_all.apply(lambda row_with_all: self.get_expat_row(row_with_all, rows_with_locals), axis=1)
        self.data = self.data.append(rows_with_expats)
        print len(self.data)

    def delete_column(self, column_name):
        logging.info("Deleting Column:" + column_name)
        self.data = self.data.drop(column_name, 1)
        print len(self.data)

    def convert_language_to_language_group(self):
        logging.info("Converting language to language group")
        self.replace_specific_key_value("languages", "French (All)", "French")
        self.replace_specific_key_value("languages","English (All)","English")
        self.replace_specific_key_value("languages", "Spanish (All),Portuguese (All),Italian,German", "European")
        self.replace_specific_key_value("languages", "Hindi,Urdu,Bengali,Tamil,Nepali,Punjabi,Telugu,Sinhala", "Indian")
        self.replace_specific_key_value("languages", "Indonesian,Filipino,Malayalam,Thai", "SE Asia")
        print len(self.data)

    def delete_all_unnamed_columns(self):
        logging.info("Delete all unnamed columns")
        for column in self.data.columns:
            if "Unnamed" in column:
                self.delete_column(column)

    def compress(self):
        pass

    def list_unique_topics(self):
        logging.info("Unique Topics")
        logging.info(self.data["analysis_name"].unique())

    def generate_facebook_population_file_in_current_data(self):
        logging.info("Save Facebook Population file")
        instances_facebook_population = self.data[self.data[TOPIC] == NULL_VALUE]
        instances_facebook_population.to_csv(APPLICATION_CURRENT_DATA_FOLDER + "facebook_population.csv")


    def generate_file_for_combination(self,combination):
        if len(combination) == 2:
            filtered_dataframe = self.data[(self.data[TOPIC] == combination[0]) | (self.data[TOPIC] == combination[1])]
            filtered_dataframe = filtered_dataframe[(filtered_dataframe[GENDER] != ALL_VALUE) & (filtered_dataframe[AGE_RANGE] != ALL_VALUE) &  (filtered_dataframe[SCHOLARITY] != ALL_VALUE) & (filtered_dataframe[CITIZENSHIP] != ALL_VALUE) ]
            filtered_dataframe.to_csv(APPLICATION_CURRENT_DATA_FOLDER + combination[0] + "-" + combination[1] + ".csv")
        elif len(combination) == 1:
            filtered_dataframe = self.data[(self.data[TOPIC] == combination[0])]
            filtered_dataframe = filtered_dataframe[
                (filtered_dataframe[GENDER] != ALL_VALUE) & (filtered_dataframe[AGE_RANGE] != ALL_VALUE) & (
                filtered_dataframe[SCHOLARITY] != ALL_VALUE) & (filtered_dataframe[CITIZENSHIP] != ALL_VALUE)]
            filtered_dataframe.to_csv(APPLICATION_CURRENT_DATA_FOLDER + combination[0] + ".csv")
        else:
            import ipdb;ipdb.set_trace()
            raise Exception("No combination found")

    def generate_combinations_files_in_current_data(self):
        logging.info("Generating Combinations Files")
        interest_list = self.data["topic"].unique().tolist()
        interest_list.remove(NULL_VALUE)
        for combination in itertools.combinations(interest_list,2):
            print combination[0],combination[1]
            self.generate_file_for_combination(combination)
        for interest in interest_list:
            self.generate_file_for_combination((interest,))

    def remove_all_languages(self):
        self.data = self.data[self.data["languages"] == NULL_VALUE]
        logging.info("Removing All Languages")

    def remove_all_citizenship(self):
        self.data = self.data[self.data["citizenship"] != NULL_VALUE]
        logging.info("Removing All Citizenship")

    def zip_folder(self):
        logging.info("Saving zipped folder")
        zipf = zipfile.ZipFile('data_' + CURRENT_DATE_SERIAL + '.zip', 'w', zipfile.ZIP_DEFLATED)
        zipdir(APPLICATION_CURRENT_DATA_FOLDER, zipf)
        zipf.close()

    def generate_topic_column(self):
        logging.info("Creating citizenship column")
        self.data[TOPIC] = self.data.apply(lambda row: get_topic_from_row(row), axis=1)

    def generate_citizenship_column(self):
        logging.info("Creating topic column")
        self.data[CITIZENSHIP] = self.data.apply(lambda row: get_citizenship_from_row(row), axis=1)

    def generate_country_code_column(self):
        logging.info("Creating country code column")
        self.data[COUNTRY_CODE] = self.data.apply(lambda row: get_country_code_from_row(row), axis=1)

    def generate_scholarity_column(self):
        logging.info("Creating scholarity column")
        self.data[SCHOLARITY] = self.data.apply(lambda row: get_scholarity_from_row(row), axis=1)

    def convert_values_to_string(self):
        self.data = self.data.applymap(str)

    def save_original_data(self):
        logging.info("Saving compressed original data")
        self.original_data["timestamp"] = time.time()
        self.original_data.to_csv(APPLICATION_CURRENT_DATA_FOLDER + "original_data.gz", compression="gzip")

    def add_and_save_at_history_folder(self):
        logging.info("Saving history")
        history_file = open(HISTORY_MAP_FILE_PATH, "a")
        new_uri = HISTORY_FOLDER_NAME + CURRENT_DATE_SERIAL + "-" + UNIQUE_TIME_ID
        to_save_path = HISTORY_FOLDER_PATH + CURRENT_DATE_SERIAL + "-" + UNIQUE_TIME_ID
        current_date = CURRENT_DATE_SERIAL
        current_time = UNIQUE_TIME_ID
        history_file.write(",".join([new_uri,current_date,current_time]) + "\n")
        shutil.copytree(APPLICATION_CURRENT_DATA_FOLDER, to_save_path)



    def process_data(self):
        self.delete_column(RESPONSE)
        self.rename_column("ages_ranges", AGE_RANGE)
        self.rename_column("scholarities", SCHOLARITY)
        self.rename_column("behavior", CITIZENSHIP)
        self.rename_column("genders", GENDER)
        self.replace_specific_key_value(GENDER, 1, "Male")
        self.replace_specific_key_value(GENDER, 2, "Female")
        self.generate_topic_column()
        self.generate_citizenship_column()
        self.generate_country_code_column()
        self.generate_scholarity_column()
        self.generate_age_range_column()
        self.replace_null_values()
        self.convert_values_to_string()
        self.delete_column(INTEREST)
        self.delete_column(TARGETING)
        self.delete_column(LANGUAGES)
        self.delete_column("all_fields")
        self.delete_column("family_statuses")
        self.delete_column("name")
        self.delete_column("geo_locations")
        self.generate_combinations_files_in_current_data()
        self.generate_facebook_population_file_in_current_data()
        # self.save_original_data()
        self.add_and_save_at_history_folder()
        # self.zip_folder()

    def save_file(self,filename):
        logging.info("Saving file: {}".format(filename))
        self.data.to_csv(filename)

    def export_json(self, filter, filename):
        logging.info("Saving json: {}".format(filename))
        json_dataset = pd.DataFrame()
        for key in filter:
            for value in filter[key]:
                json_dataset = json_dataset.append(self.data[self.data[key] == value])
        json_dataset.to_json(filename, orient="records")

    def append_dataset_save(self,datasetname):
        new_data = pd.read_csv(datasetname)
        self.data = self.data.append(new_data,ignore_index=True)
        self.data.to_csv("new_dataset.csv")

    def __init__(self, filepath):
        logging.info("Loading Data... (take some seconds)")
        self.data = load_dataframe_from_file(filepath)
        self.original_data = self.data.copy(deep=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='JSON Query File')
    args = parser.parse_args()
    if args.input:
        pd_dataset = PostProcessVisualizationData(args.input)
        pd_dataset.process_data()
    else:
        raise Exception("No input data")
