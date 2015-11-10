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
'''
from myclass.class_read_text_to_database import *
import os.path
import ConfigParser
'''
################################ PART3 MAIN ###########################################
def main():
    # class_initialization_and_load_parameter
    config_data_dir = "./config.ini"
    log_data_dir = "./main.log"
    ParameterLoader = InitializationAndLoadParameter(log_data_dir = log_data_dir)

    appName, log_data_dir, database_name, database_password,\
    message_table_name, word_table_name, train_data_dir,\
    test_data_dir, stopword_data_dir = ParameterLoader.load_parameter(config_data_dir = config_data_dir)



    # class_create_database_table
    Creater = createDatabaseTable(log_data_dir = log_data_dir)
    Creater.create_database(database_name = database_name)
    Creater.create_table(database_name = database_name,\
                         message_table_name = message_table_name,\
                         word_table_name = word_table_name)



    # class_read_text_to_database


################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()