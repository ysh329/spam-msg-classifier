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
    def __init__(self, database_name, pyspark_sc):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = './save_word_main.log',
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

        # Configure Spark
        try:
            self.sc = pyspark_sc
            logging.info("Start pyspark successfully.")
        except Exception as e:
            logging.error("Fail in starting pyspark.")
            logging.error(e)


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



    def get_message_rdd_from_database(self, database_name, message_table_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        sqls.append("SELECT id, split_result_clean_string FROM {database_name}.{table_name} WHERE id < 1000".format(database_name = database_name, table_name = message_table_name))
        #sqls.append("SELECT id, split_result_clean_string FROM {database_name}.{table_name}".format(database_name = database_name, table_name = message_table_name))
        for idx in xrange(len(sqls)):
            sql = sqls[idx]
            try:
                cursor.execute(sql)
                if idx == len(sqls)-1:
                    id_and_clean_string_2d_tuple = cursor.fetchall()
                    logging.info("len(id_and_clean_string_2d_tuple):{0}".format(len(id_and_clean_string_2d_tuple)))
                    logging.info("id_and_clean_string_2d_tuple[0]:{0}".format(id_and_clean_string_2d_tuple[0]))
                    logging.info("id_and_clean_string_2d_tuple[:3]:{0}".format(id_and_clean_string_2d_tuple[:3]))

                    id_and_clean_string_list_message_rdd = self.sc.parallelize(id_and_clean_string_2d_tuple).map(lambda (id, split_result_clean_string): (int(id), split_result_clean_string.split("///")) )
                    logging.info("id_and_clean_string_list_message_rdd.count():{0}".format(id_and_clean_string_list_message_rdd.count()))
                    logging.info("id_and_clean_string_list_message_rdd.take(1):{0}".format(id_and_clean_string_list_message_rdd.take(1)))
            except MySQLdb.Error, e:
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()

        return id_and_clean_string_list_message_rdd



    def get_word_rdd_from_database(self, database_name, word_table_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        sqls.append("SELECT id, word FROM {database_name}.{table_name}".format(database_name = database_name, table_name = word_table_name))

        for idx in xrange(len(sqls)):
            sql = sqls[idx]
            try:
                cursor.execute(sql)
                if idx == len(sqls)-1:
                    id_and_word_2d_tuple = cursor.fetchall()
                    logging.info("len(id_and_word_2d_tuple):{0}".format(len(id_and_word_2d_tuple)))
                    logging.info("id_and_word_2d_tuple[0]:{0}".format(id_and_word_2d_tuple[0]))
                    logging.info("id_and_word_2d_tuple[:2]:{0}".format(id_and_word_2d_tuple[:2]))

                    id_and_word_rdd = self.sc.parallelize(id_and_word_2d_tuple).map(lambda (id, word): (int(id), word))
                    logging.info("id_and_word_rdd.count():{0}".format(id_and_word_rdd.count()))
                    logging.info("id_and_word_rdd.take(1):{0}".format(id_and_word_rdd.take(1)))
            except MySQLdb.Error, e:
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()

        return id_and_word_rdd



    def string_to_word_vector(self, id_and_clean_string_list_message_rdd, id_and_word_rdd):
        pass


    def string_to_index_vector(self, word_string):
        pass



    def index_vector_to_word_vector(self, index_vector_list):
        pass



################################### PART3 CLASS TEST ##################################
# Initialization Parameters
database_name = "messageDB"
message_table_name = "message_table"
from pyspark import SparkContext, SparkConf

pyspark_sc = SparkContext("")

Word2Vec = String2WordVec(database_name = database_name, pyspark_sc = pyspark_sc)
id_and_clean_string_list_message_rdd = Word2Vec.get_message_rdd_from_database(database_name = database_name, message_table_name = message_table_name)
