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



    # (id, is_train, true_label, word_num, content, split_result_string, split_result_num, split_result_clean_num)
    def read_text_into_meta_data(self):
        logging.info("")
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
        # (id, is_train, true_label, word_num, content, split_result_string, split_result_num, split_result_clean_string, split_result_clean_num)
        # id
        id_list = map(lambda record: record[0].strip(), train_data_triple_2d_list)
        # is_train
        is_train_list = map(lambda record: 1 if len(record) == 3 else 0, train_data_triple_2d_list)
        # true_label
        true_label_list = map(lambda record: record[1].strip(), train_data_triple_2d_list)
        # word_num
        word_num_list = map(lambda record: len(record[2].decode("utf8")), train_data_triple_2d_list)
        # content
        content_list = map(lambda record: record[2].strip(), train_data_triple_2d_list)

        split_result_2d_list = map(lambda message: list(jieba.cut(message)), content_list)
        # split_result_string
        split_result_string_list = map(lambda result_list: "///".join(result_list), split_result_2d_list)
        # split_result_num
        split_result_num_list = map(lambda result_list: len(result_list), split_result_2d_list)

        logging.info("len(id_list): %s." % len(id_list))
        logging.info("type(id_list[0]): %s." % type(id_list[0]))
        logging.info("id_list[0]: %s." % id_list[0])
        logging.info("id_list[len(id_list)-1]: %s." % id_list[len(id_list)-1])
        logging.info("")
        logging.info("len(is_train_list): %s." % len(is_train_list))
        logging.info("type(is_train_list[0]): %s." % type(is_train_list[0]))
        logging.info("is_train_list[0]: %s." % is_train_list[0])
        logging.info("is_train_list[len(is_train_list)-1]: %s." % is_train_list[len(is_train_list)-1])
        logging.info("")
        logging.info("len(true_label_list): %s." % len(true_label_list))
        logging.info("type(true_label_list[0]): %s." % type(true_label_list[0]))
        logging.info("true_label_list[0]: %s." % true_label_list[0])
        logging.info("true_label_list[len(true_label_list)-1]: %s." % true_label_list[len(true_label_list)-1])
        logging.info("")
        logging.info("len(content_list): %s." % len(content_list))
        logging.info("type(content_list[0]): %s." % type(content_list[0]))
        logging.info("content_list[0]: %s." % content_list[0])
        logging.info("content_list[len(content_list)-1]: %s." % content_list[len(content_list)-1])
        logging.info("")
        logging.info("len(word_num_list): %s." % len(word_num_list))
        logging.info("type(word_num_list[0]): %s." % type(word_num_list[0]))
        logging.info("word_num_list[0]: %s." % word_num_list[0])
        logging.info("word_num_list[len(word_num_list)-1]: %s." % word_num_list[len(word_num_list)-1])
        logging.info("")
        logging.info("len(split_result_string_list): %s." % len(split_result_string_list))
        logging.info("type(split_result_string_list[0]): %s." % type(split_result_string_list[0]))
        logging.info("split_result_string_list[0]: %s." % split_result_string_list[0])
        logging.info("split_result_string_list[len(split_result_string_list)-1]: %s." % split_result_string_list[len(split_result_string_list)-1])
        logging.info("")
        logging.info("len(split_result_2d_list): %s." % len(split_result_2d_list))
        logging.info("type(split_result_2d_list[0]): %s." % type(split_result_2d_list[0]))
        logging.info("split_result_2d_list[0]: %s." % split_result_2d_list[0])
        logging.info("split_result_2d_list[len(split_result_2d_list)-1]: %s." % split_result_2d_list[len(split_result_2d_list)-1])
        logging.info("")
        logging.info("len(split_result_num_list): %s." % len(split_result_num_list))
        logging.info("type(split_result_num_list[0]): %s." % type(split_result_num_list[0]))
        logging.info("split_result_num_list[0]: %s." % split_result_num_list[0])
        logging.info("split_result_num_list[len(split_result_num_list)-1]: %s." % split_result_num_list[len(split_result_num_list)-1])

        return id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list, split_result_2d_list



    def save_meta_data_to_database(self, database_name, table_name, id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list):
        logging.info("")
        cursor = self.con.cursor()
        sqls = ['USE %s' % database_name, 'SET NAMES UTF8']
        sqls.append("ALTER DATABASE %s DEFAULT CHARACTER SET 'utf8'" % database_name)

        for id_idx in xrange(len(id_list)):
            id = id_list[id_idx]
            is_train = is_train_list[id_idx]
            true_label = true_label_list[id_idx]
            word_num = word_num_list[id_idx]
            content =content_list[id_idx]
            split_result_string = split_result_string_list[id_idx]
            split_result_num = split_result_num_list[id_idx]

            sqls.append("""INSERT INTO %s(id, is_train, true_label, word_num, content, split_result_string, split_result_num)
                                          VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (table_name, id, is_train, true_label, word_num, content, split_result_string, split_result_num))


        try:
            '''
            for sql_idx in range(len(sqls)):
                sql = sqls[sql_idx]
                cursor.execute(sql)
            '''
            map(lambda sql: cursor.execute(sql), sqls)
            self.con.commit()
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
        finally:
            cursor.close()



    def read_text_into_clean_data(self, split_result_2d_list):
        logging.info("")
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



################################### PART3 CLASS TEST ##################################
# initial parameters
database_name = "messageDB"
table_name_list = ["message_table", "word_table"]
train_data_dir = "../data/input/train_set_80W.txt"
stopword_data_dir = "../data/input/stopword.txt"


Reader = ReadText2DB(database_name = database_name,
                     train_data_dir = train_data_dir,
                     stopword_data_dir = stopword_data_dir)
id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list, split_result_2d_list = Reader.read_text_into_meta_data()
Reader.save_meta_data_to_database(database_name, table_name_list[0], id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list)
