# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_database_table.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-10-22 21:03:49
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import MySQLdb
import logging
import time

################################### PART2 CLASS && FUNCTION ###########################
class createDatabaseTable(object):
    def __init__(self, log_data_dir):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = log_data_dir,#'./save_word_main.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START CLASS {class_name}.".format(class_name = createDatabaseTable.__name__))

        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))



    def __del__(self):
        self.con.close()

        logging.info("Success in quiting MySQL.")
        logging.info("END CLASS {class_name}.".format(class_name = createDatabaseTable.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = createDatabaseTable.__name__, delta_time = self.end - self.start))



    def create_database(self, database_name):
        logging.info("database name: {database_name}".format(database_name = database_name))

        cursor = self.con.cursor()
        sqls = ['SET NAMES UTF8', 'SELECT VERSION()', "CREATE DATABASE {database_name}".format(database_name = database_name)]

        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                if sql_idx == 1:
                    result = cursor.fetchall()[0]
                    mysql_version = result[0]
                    logging.info("MySQL VERSION: {mysql_version}".format(mysql_version = mysql_version))
                self.con.commit()
                logging.info("Success in creating database {database_name}.".format(database_name = database_name))
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in creating database {database_name}.".format(database_name = database_name))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()



    def create_table(self, database_name, message_table_name, word_table_name):
        cursor = self.con.cursor()
        sqls = ["USE {database_name}".format(database_name = database_name), 'SET NAMES UTF8']

        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))

        # Create message_table_name
        sqls.append("""CREATE TABLE IF NOT EXISTS {message_table_name}(
                                id INT(11) NOT NULL,
                                is_train INT(11),
                                true_label INT(11),
                                predicted_label INT(11),
                                is_spam_prob FLOAT,
                                word_num INT(11),
                                keyword1 TEXT,
                                keyword2 TEXT,
                                keyword3 TEXT,
                                number_num INT(11),
                                letter_num INT(11),
                                symbol_num INT(11),
                                content TEXT NOT NULL,
                                split_result_string TEXT,
                                split_result_num INT(11),
                                split_result_clean_string TEXT,
                                split_result_clean_num INT(11),
                                stopword_num INT(11),
                                word_index_string TEXT,
                                word_vector_string TEXT,
                                UNIQUE (id))""".format(message_table_name = message_table_name))
        sqls.append("CREATE INDEX id_idx ON {message_table_name}(id)".format(message_table_name = message_table_name))

        # Create word_table_name
        sqls.append("""CREATE TABLE IF NOT EXISTS {word_table_name}(
                                id INT(11) AUTO_INCREMENT PRIMARY KEY,
                                word VARCHAR(100),
                                is_stopword INT(11),
                                word_length INT(11),
                                topic TEXT,
                                true_pos_num INT(11),
                                true_neg_num INT(11),
                                all_num INT(11),
                                true_neg_pro FLOAT,
                                predicted_pos_num INT(11),
                                predicted_neg_num INT(11),
                                UNIQUE (word))""".format(word_table_name = word_table_name))
        sqls.append("CREATE INDEX id_idx ON {word_table_name}(id)".format(word_table_name = word_table_name))
        sqls.append("CREATE INDEX word_idx ON {word_table_name}(word)".format(word_table_name = word_table_name))

        for sql_idx in range(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                self.con.commit()
                logging.info("Success in creating table.")
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in creating table.")
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()



################################### PART3 CLASS TEST ##################################
"""
# initial parameters
database_name = "messageDB"
message_table_name = "message_table"
word_table_name = "word_table"
log_data_dir = "./save_word_main.log"

Creater = createDatabaseTable(log_data_dir = log_data_dir)
Creater.create_database(database_name = database_name)
Creater.create_table(database_name = database_name,\
                     message_table_name = message_table_name,\
                     word_table_name = word_table_name)
"""