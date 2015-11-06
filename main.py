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
from myclass.class_create_database_table import *
from myclass.class_read_text_to_database import *
import os.path
import ConfigParser
################################ PART3 MAIN ###########################################
def main():

    conf = ConfigParser.ConfigParser()
    conf.read("./config.ini")
    appName = conf.get("basic", "appName")
    logging.info("appName: {appName}".format(appName = appName))

    database_name = conf.get("database", "database_name")
    database_password = conf.get("database", "database_password")
    message_table_name = conf.get("database", "message_table_name")
    word_table_name = conf.get("database", "word_table_name")

    logging.info("database_name: {database_name}".format(database_name = database_name))
    logging.info("database_password: {database_password}".format(database_password = database_password))
    logging.info("message_table_name: {message_table_name}".format(message_table_name = message_table_name))
    logging.info("word_table_name: {word_table_name}".format(word_table_name = word_table_name))

    train_data_dir = conf.get("data", "train_data_dir")
    test_data_dir = conf.get("data", "test_data_dir")
    stopword_data_dir = conf.get("data", "stopword_data_dir")

    logging.info("train_data_dir: {train_data_dir}".format(train_data_dir = train_data_dir))
    logging.info("test_data_dir: {test_data_dir}".format(test_data_dir = test_data_dir))
    logging.info("stopword_data_dir: {stopword_data_dir}".format(stopword_data_dir = stopword_data_dir))

################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()