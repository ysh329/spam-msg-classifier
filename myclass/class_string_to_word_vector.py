# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_string_to_word_vector.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-11-13 01:35:36
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import time
import MySQLdb

################################### PART2 CLASS && FUNCTION ###########################
class String2WordVec(object):
    def __init__(self, database_name):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = './main.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START CLASS {class_name}.".format(class_name = String2WordVec.__name__))

        # connect database
        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', db = database_name, charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))



    def __del__(self):
        # close database
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except Exception as e:
            logging.error(e)

        logging.info("END CLASS {class_name}.".format(class_name = String2WordVec.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : %0.3{delta_time} seconds".format(class_name = String2WordVec.__name__, delta_time = self.end))



    def string_to_index_vector(self, word_string):
        pass



    def index_vector_to_word_vector(self, index_vector_list):
        pass

################################### PART3 CLASS TEST ##################################
# Initialization Parameters
database_name = "messageDB"

Word2Vec = String2WordVec(database_name = database_name)