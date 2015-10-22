# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_read_text_to_database.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-10-22 21:17:57
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import MySQLdb
import time

################################### PART2 CLASS && FUNCTION ###########################
class ReadText2DB(object):

    def __init__(self, database_name, train_data_dir):
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

        # read text
        try:
            self.train_f = open(train_data_dir, 'r')
            logging.info("Success in reading %s." % train_data_dir)
        except Exception as e:
            logging.error("Fail in reading %s." % train_data_dir)

        # connect database
        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', db = database_name, charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))



    def __del__(self):
        self.train_f.close()
        self.con.close()

        logging.info("Success in closing file.")
        logging.info("Success in quiting MySQL.")
        logging.info("END.")

        self.end = time.clock()
        logging.info("The function run time is : %.03f seconds" % (self.end - self.start))



    def read_to_variable(self):
        try:
            train_line_list = self.train_f.readlines()
            logging.info("Success in reading file to variable.")
            logging.info("type(train_line_list): %s" % type(train_line_list))
            logging.info("len(train_line_list): %s" % len(train_line_list))
            logging.info("train_line_list[0]: %s" % train_line_list[0])
            logging.info("train_line_list[len(train_line_list)-1]: %s" % train_line_list[len(train_line_list)-1])
        except Exception as e:
            logging.error(e)
            return

        '''
        result_list = train_line_list[0].split("\t")
        for i in result_list: print i
        '''
        train_data_triple_2d_list = map(lambda line: line.split("\t"), train_line_list)
        print "len(train_data_triple_2d_list): %s." % len(train_data_triple_2d_list)
        print "train_data_triple_2d_list[0]: %s." % train_data_triple_2d_list[0]
        for i in train_data_triple_2d_list[0]: print i
        print "train_data_triple_2d_list[len(train_data_triple_2d_list)-1]: %s." % train_data_triple_2d_list[len(train_data_triple_2d_list)-1]
        for i in train_data_triple_2d_list[len(train_data_triple_2d_list)-1]: print i





################################### PART3 CLASS TEST ##################################
# initial parameters
database_name = "messageDB"
train_data_dir = "../data/input/train_set_80W.txt"

Reader = ReadText2DB(database_name, train_data_dir)
Reader.read_to_variable()