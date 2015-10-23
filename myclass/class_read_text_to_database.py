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
import jieba

################################### PART2 CLASS && FUNCTION ###########################
class ReadText2DB(object):

    def __init__(self, database_name, train_data_dir, stopword_data_dir):
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

        # read training text
        try:
            self.train_f = open(train_data_dir, 'r')
            logging.info("Success in reading %s." % train_data_dir)
        except Exception as e:
            logging.error("Fail in reading %s." % train_data_dir)

        # read stopword text
        try:
            self.stopword_f = open(stopword_data_dir, 'r')
            logging.info("Success in reading %s." % stopword_data_dir)
        except Exception as e:
            logging.error("Fail in reading %s." % stopword_data_dir)

        # connect database
        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', db = database_name, charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))



    def __del__(self):
        self.train_f.close()
        self.stopword_f.close()
        self.con.close()

        logging.info("Success in closing file.")
        logging.info("Success in quiting MySQL.")
        logging.info("END.")

        self.end = time.clock()
        logging.info("The function run time is : %.03f seconds" % (self.end - self.start))


    # (id, istrain, true_label, word_num, content, split_result_string, split_result_num, split_result_clean_num)
    def read_to_variable(self):
        try:
            stopword_list = self.stopword_f.readlines()
            logging.info("Success in reading file to variable.")
            logging.info("type(stopword_list): %s" % type(stopword_list))
            logging.info("len(stopword_list): %s" % len(stopword_list))
            logging.info("stopword_list[0]: %s" % stopword_list[0])
            logging.info("stopword_list[len(stopword_list)-1]: %s" % stopword_list[len(stopword_list)-1])
        except Exception as e:
            logging.error(e)
            stopword_list = []

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
        logging.info("len(train_data_triple_2d_list): %s." % len(train_data_triple_2d_list))
        logging.info("train_data_triple_2d_list[0]: %s." % train_data_triple_2d_list[0])
        logging.info("len(train_data_triple_2d_list[0]): %s." % len(train_data_triple_2d_list[0][2]))
        #for i in train_data_triple_2d_list[0]: print i
        logging.info("train_data_triple_2d_list[len(train_data_triple_2d_list)-1]: %s." % train_data_triple_2d_list[len(train_data_triple_2d_list)-1])
        #for i in train_data_triple_2d_list[len(train_data_triple_2d_list)-1]: print i

        # clean data
        # (id, istrain, true_label, word_num, content, split_result_string, split_result_num, split_result_clean_num)
        id_list = map(lambda record: record[0].strip(), train_data_triple_2d_list)
        true_label_list = map(lambda record: record[1].strip(), train_data_triple_2d_list)
        word_num_list = map(lambda record: len(record[2].decode("utf8")), train_data_triple_2d_list)
        content_list = map(lambda record: record[2].strip(), train_data_triple_2d_list)
        split_result_2d_list = map(lambda message: jieba.cut(message), content_list)
        split_result_string_list = map(lambda result_list: "/".join(result_list), split_result_2d_list)
        split_result_num_list = map(lambda result_list: len(result_list), split_result_2d_list)
        seg_list = jieba.cut(content_list[9])
        for i in seg_list: print i


        logging.info("len(id_list): %s." % len(id_list))
        logging.info("type(id_list[0]): %s." % type(id_list[0]))
        logging.info("id_list[0]: %s." % id_list[0])
        logging.info("id_list[len(id_list)-1]: %s." % id_list[len(id_list)-1])

        logging.info("len(true_label_list): %s." % len(true_label_list))
        logging.info("type(true_label_list[0]): %s." % type(true_label_list[0]))
        logging.info("true_label_list[0]: %s." % true_label_list[0])
        logging.info("true_label_list[len(true_label_list)-1]: %s." % true_label_list[len(true_label_list)-1])

        logging.info("len(content_list): %s." % len(content_list))
        logging.info("type(content_list[0]): %s." % type(content_list[0]))
        logging.info("content_list[0]: %s." % content_list[0])
        logging.info("content_list[len(content_list)-1]: %s." % content_list[len(content_list)-1])

        logging.info("len(word_num_list): %s." % len(word_num_list))
        logging.info("type(word_num_list[0]): %s." % type(word_num_list[0]))
        logging.info("word_num_list[0]: %s." % word_num_list[0])
        logging.info("word_num_list[len(word_num_list)-1]: %s." % word_num_list[len(word_num_list)-1])

        logging.info("len(split_result_string_list): %s." % len(split_result_string_list))
        logging.info("type(split_result_string_list[0]): %s." % type(split_result_string_list[0]))
        logging.info("split_result_string_list[0]: %s." % split_result_string_list[0])
        logging.info("split_result_string_list[len(split_result_string_list)-1]: %s." % split_result_string_list[len(split_result_string_list)-1])
################################### PART3 CLASS TEST ##################################
# initial parameters
database_name = "messageDB"
train_data_dir = "../data/input/train_set_80W.txt"
stopword_data_dir = "../data/input/stopword.txt"

Reader = ReadText2DB(database_name = database_name,
                     train_data_dir = train_data_dir,
                     stopword_data_dir = stopword_data_dir)
Reader.read_to_variable()