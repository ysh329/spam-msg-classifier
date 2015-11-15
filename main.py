# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: main.py
# Description:

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-10-25 22:20:14
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
from myclass.class_initialization_and_load_parameter import *
from myclass.class_create_database_table import *
from myclass.class_save_word_to_database import *
################################ PART3 MAIN ###########################################
def main():
    # class_initialization_and_load_parameter
    config_data_dir = "./config.ini"
    log_data_dir = "./main.log"
    ParameterLoader = InitializationAndLoadParameter(log_data_dir = log_data_dir)

    pyspark_app_name, log_data_dir, database_name, database_password,\
    message_table_name, word_table_name, train_data_dir,\
    test_data_dir, stopword_data_dir = ParameterLoader.load_parameter(config_data_dir = config_data_dir)



    # class_create_database_table
    Creater = createDatabaseTable(log_data_dir = log_data_dir)
    Creater.create_database(database_name = database_name)
    Creater.create_table(database_name = database_name,\
                         message_table_name = message_table_name,\
                         word_table_name = word_table_name)



    # class_read_text_to_database




    # class_save_word_to_database
    WordRecord = UniqueWordSaver(database_name = database_name,
                                 stopword_data_dir = stopword_data_dir,
                                 pyspark_app_name = pyspark_app_name)
    WordRecord.save_stopword_to_database(database_name = database_name,
                                         word_table_name = word_table_name)
    slash_split_string_1d_tuple = WordRecord.read_split_result_string_from_database(database_name = database_name,
                                                                                    message_table_name = message_table_name)
    split_string_with_stopword_1d_tuple_rdd = WordRecord.generate_split_string_with_stopword_1d_tuple_rdd(slash_split_string_1d_tuple = slash_split_string_1d_tuple,
                                                                                                          pyspark_app_name = pyspark_app_name)
    word_count_rdd = WordRecord.word_count_for_split_string_1d_tuple_rdd(split_string_with_stopword_1d_tuple_rdd = split_string_with_stopword_1d_tuple_rdd)
    word_count_len_is_stopword_rdd = WordRecord.compute_len_is_stopword_rdd(word_count_rdd = word_count_rdd)
    WordRecord.save_word_count_with_len_rdd_to_database(database_name = database_name,
                                                        word_table_name = word_table_name,
                                                        word_count_len_is_stopword_rdd = word_count_len_is_stopword_rdd)



    # class_string_to_word_vector

################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()