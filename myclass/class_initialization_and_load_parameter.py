# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_initialization_and_load_parameter.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-11-10 10:20:19
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import ConfigParser
import time
################################### PART2 CLASS && FUNCTION ###########################
class InitializationAndLoadParameter(object):
    def __init__(self):
        self.start = time.clock()

        logging.basicConfig(level = logging.DEBUG,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = './main.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START.")



    def __del__(self):
        logging.info("Success in closing file.")
        logging.info("Success in quiting MySQL.")
        logging.info("END.")

        self.end = time.clock()
        logging.info("The function run time is : %.03f seconds" % (self.end - self.start))



    def load_parameter(self, config_data_dir):
        conf = ConfigParser.ConfigParser()
        conf.read(config_data_dir)
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

        return appName, database_name, database_password, message_table_name, word_table_name, train_data_dir, test_data_dir, stopword_data_dir


################################### PART3 CLASS TEST ##################################
"""
config_data_dir = "../config.ini"
ParameterLoader = InitializationAndLoadParameter()

appName, database_name, database_password,\
message_table_name, word_table_name,\
train_data_dir, test_data_dir,\
stopword_data_dir = ParameterLoader.load_parameter(config_data_dir = config_data_dir)
"""