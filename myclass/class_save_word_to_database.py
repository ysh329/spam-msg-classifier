# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_save_word_to_database.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-11-09 13:48:08
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import MySQLdb
import time
from pyspark import SparkContext
################################### PART2 CLASS && FUNCTION ###########################
class UniqueWordSaver(object):
    def __init__(self, database_name, stopword_data_dir):
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
        logging.info("START CLASS {class_name}.".format(class_name = UniqueWordSaver.__name__))

        # connect database
        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', db = database_name, charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        # open stop word file
        try:
            self.stopword_f = open(stopword_data_dir, 'r')
            logging.info("Open stop word file({stopword_data_dir}) successfully.".format(stopword_data_dir = stopword_data_dir))
        except Exception as e:
            logging.error(e)



    def __del__(self):
        # close database
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except Exception as e:
            logging.error(e)

        # close stop word file
        try:
            self.stopword_f.close()
            logging.info("close stop word file({stopword_data_dir}) successfully.".format(stopword_data_dir = stopword_data_dir))
        except Exception as e:
            logging.error(e)

        logging.info("END CLASS {class_name}.".format(class_name = UniqueWordSaver.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : %0.3{delta_time} seconds".format(class_name = UniqueWordSaver.__name__, delta_time = self.end))



    def save_stopword_to_database(self, database_name, word_table_name):
        # word: stopword
        try:
            self.stopword_list = list(set(map(lambda stopword: stopword.strip(), self.stopword_f.readlines())))
            self.stopword_list[0] = " "
            logging.info("Success in reading file to variable.")
            logging.info("type(stopword_list): {stopword_list_type}".format(stopword_list_type = type(self.stopword_list)))
            logging.info("len(stopword_list): {stopword_list_length}".format(stopword_list_length = len(self.stopword_list)))
            logging.info("stopword_list[0]: {dot}{first_blank_stopword}{dot}".format(dot = ".", first_blank_stopword = self.stopword_list[0]))
            logging.info("stopword_list[len(stopword_list)-1]: {last_stopword}".format(last_stopword = self.stopword_list[len(self.stopword_list)-1]))
        except Exception as e:
            logging.error(e)
            self.stopword_list = []

        # word_length: word_length_list
        try:
            word_length_list = map(lambda stopword: len(stopword.decode('utf8')), self.stopword_list)
            logging.info("len(word_length_list): {word_list_length}.".format(word_list_length = len(word_length_list)))
            logging.info("word_length_list[0]: {first_word_length}.".format(first_word_length = word_length_list[0]))
            logging.info("type(word_length_list[8]): {first_word_type}.".format(first_word_type = type(word_length_list[8])))
            logging.info("word_length_list[len(word_length_list)-1]: {last_word_length}.".format(last_word_length = word_length_list[len(word_length_list)-1]))
        except Exception as e:
            logging.error(e)

        # SQL generator
        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        nonstopword_sqls_length = len(sqls)

        for stopword_idx in xrange(len(self.stopword_list)):

            # is_stopword
            is_stopword = 1

            stopword = self.stopword_list[stopword_idx]
            word_length = word_length_list[stopword_idx]

            if not self.check_repeat_word_in_database_table(word = stopword, database_name = database_name, table_name = word_table_name):
                try:
                    if stopword == "'":
                        sql = """INSERT INTO {database_name}.{word_table_name}(word, is_stopword, word_length) VALUES("{stopword}", {is_stopword}, {word_length})"""\
                                   .format(database_name = database_name, word_table_name = word_table_name, stopword = stopword, is_stopword = is_stopword, word_length = word_length)
                    else:
                        sql = """INSERT INTO {database_name}.{word_table_name}(word, is_stopword, word_length) VALUES("{stopword}", {is_stopword}, {word_length})"""\
                                .format(database_name = database_name, word_table_name = word_table_name, stopword = stopword, is_stopword = is_stopword, word_length = word_length)
                    sqls.append(sql)
                except Exception as e:
                    logging.error(e)
        logging.info("len(sqls): {sqls_list_length}.".format(sqls_list_length = len(sqls)))
        logging.info("sqls[0]: {first_sql}.".format(first_sql = sqls[0]))
        logging.info("sqls[len(sqls)-1]: {last_sql}.".format(last_sql = sqls[len(sqls)-1]))

        if len(sqls) == nonstopword_sqls_length:
            logging.info("All stop words has inserted before.")
            return

        # SQL executor
        success_insert = 0
        failure_insert = 0
        cursor = self.con.cursor()
        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                #map(lambda sql: cursor.execute(sql), sqls)
                self.con.commit()
                success_insert = success_insert + 1
            except MySQLdb.Error, e:
                failure_insert = failure_insert + 1
                self.con.rollback()
                logging.error("Error SQL: {sql}.".format(sql = sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()
        logging.info("success_insert: {success_insert}.".format(success_insert = success_insert - nonstopword_sqls_length))
        logging.info("failure_insert: {failure_insert}.".format(failure_insert = failure_insert))



    def check_repeat_word_in_database_table(self, word, database_name, table_name):
        word_is_repeat = 0

        cursor = self.con.cursor()

        sql = """SELECT id FROM {database_name}.{table_name} WHERE word='{word}'""".format(database_name = database_name, table_name = table_name, word = word)
        if word == "'":
            sql = """SELECT id FROM {database_name}.{table_name} WHERE word="{word}" """.format(database_name = database_name, table_name = table_name, word = word)

        try:
            cursor.execute(sql)
            check_id_tuple = cursor.fetchall()
            cursor.close()
            logging.info("check_id_tuple: {check_id_tuple}".format(check_id_tuple = check_id_tuple))
            if len(check_id_tuple) > 0:
                word_is_repeat = 1
                return word_is_repeat
            else:
                return word_is_repeat
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Error SQL: {sql}.".format(sql = sql))
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))



    def read_split_result_string_from_database(self, database_name, message_table_name, pyspark_app_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        sqls.append("""SELECT split_result_string FROM {database_name}.{message_table_name} WHERE id < 1000""".format(database_name = database_name, message_table_name = message_table_name))

        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                if sql == sqls[-1]:
                    split_result_string_tuple = cursor.fetchall()
                    logging.info("len(split_result_string_tuple): {tuple_length}.".format(tuple_length = len(split_result_string_tuple)))
                    logging.info("len(split_result_string_tuple[0]): {first_tuple_length}".format(first_tuple_length = len(split_result_string_tuple[0])))
                    logging.info(u"split_result_string_tuple[0][0]: {first_tuple_first_tuple}".format(first_tuple_first_tuple = split_result_string_tuple[0][0]))
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()

        slash_split_string_1d_tuple = map(lambda split_2d_tuple: split_2d_tuple[0], split_result_string_tuple)

        logging.info("len(slash_split_string_1d_tuple): {string_1d_tuple_length}".format(string_1d_tuple_length = len(slash_split_string_1d_tuple)))
        logging.info(u"slash_split_string_1d_tuple[0]: {first_element_in_tuple}".format(first_element_in_tuple = slash_split_string_1d_tuple[0]))
        logging.info("type(slash_split_string_1d_tuple): {first_element_type}".format(first_element_type = type(slash_split_string_1d_tuple)))

        return slash_split_string_1d_tuple



    def generate_split_string_with_stopword_1d_tuple_rdd(self, slash_split_string_1d_tuple, pyspark_app_name):
        self.sc = SparkContext(appName = pyspark_app_name)
        logging.info("SparkContext Version: {sc_version}".format(sc_version = self.sc.version))

        slash_split_string_1d_tuple_rdd = self.sc.parallelize(slash_split_string_1d_tuple)
        logging.info("slash_split_string_1d_tuple_rdd.count(): {rdd_count}".format(rdd_count = slash_split_string_1d_tuple_rdd.count()))
        logging.info("type(slash_split_string_1d_tuple_rdd): {type_rdd}".format(type_rdd = type(slash_split_string_1d_tuple_rdd)))
        logging.info("ID of slash_split_string_1d_tuple_rdd: {rdd_id}".format(rdd_id = slash_split_string_1d_tuple_rdd.id()))
        slash_split_string_1d_tuple_rdd.setName("First RDD")

        logging.info("RDD lineage of slash_split_string_1d_tuple_rdd: {rdd_lineage}".format(rdd_lineage = slash_split_string_1d_tuple_rdd.toDebugString()))
        logging.info("Partition number of slash_split_string_1d_tuple_rdd: {0}".format(slash_split_string_1d_tuple_rdd.getNumPartitions()))

        self.split_string_with_stopword_1d_tuple_rdd = slash_split_string_1d_tuple_rdd.map(lambda slash_split_string: tuple(slash_split_string.split("///")))
        self.split_string_with_stopword_1d_tuple_rdd.persist()
        logging.info(u"first element of split_string_with_stopword_1d_tuple_rdd: {0}".format(u"".join(self.split_string_with_stopword_1d_tuple_rdd.take(1)[0])))

        return self.split_string_with_stopword_1d_tuple_rdd



    def word_count_for_split_string_1d_tuple_rdd(self, split_string_with_stopword_1d_tuple_rdd):
        word_count_rdd = self.sc.parallelize(split_string_with_stopword_1d_tuple_rdd
                                                  .reduce(lambda tuple1,tuple2: tuple1+tuple2))\
                                                  .map(lambda word: (word, 1)\
                                                  )\
                                                 .reduceByKey(lambda w1,w2: w1+w2)\
                                                 .sortBy(lambda word_tuple: -word_tuple[1])

        self.word_len_rdd = word_count_rdd.map(lambda (word, counter): (word, len(word)))
        stopword_list = self.stopword_list
        self.word_is_stopword_rdd = word_count_rdd.map(lambda (word, counter): (word, int(word in stopword_list)))
        logging.info("self.word_len_rdd.take(5):{0}".format(self.word_len_rdd.take(5)))
        logging.info("self.word_is_stopword_rdd.take(5):{0}".format(self.word_is_stopword_rdd.take(5)))
        logging.info("type(word_count_rdd):{0}".format(type(word_count_rdd)))
        #print self.word_count_rdd.collect()
        word_count_rdd.persist()
        word_count_rdd.setName("wordCountRDD")
        word_count_num = word_count_rdd.count()
        logging.info("word_count_rdd.count(): {0}".format(word_count_num))
        return word_count_rdd



    def compute_len_is_stopword_rdd(self, word_count_rdd):

        stopword_list = self.stopword_list
        word_count_len_is_stopword_rdd = word_count_rdd.map(lambda (word, counter): (word, counter, len(word), int(word in stopword_list)))

        logging.info("word_count_len_is_stopword_rdd.take(5):{0}".format(word_count_len_is_stopword_rdd.take(5)))
        #word_count_len_is_stopword_rdd.saveAsTextFile("word_count_len_is_stopword_rdd.txt")

        logging.info("type(word_count_len_is_stopword_rdd):{0}".format(type(word_count_len_is_stopword_rdd)))

        #print word_count_len_is_stopword_rdd.collect()


        word_count_tuple_list = word_count_len_is_stopword_rdd.collect()
        for idx in xrange(len(word_count_tuple_list)):
            word_count_tuple = word_count_tuple_list[idx]
            print idx, word_count_tuple[0], word_count_tuple[1], word_count_tuple[2], word_count_tuple[3]

        return word_count_len_is_stopword_rdd



    def save_word_count_with_len_rdd_to_database(self, database_name, word_table_name, word_count_len_is_stopword_rdd):
        word_count_len_is_stopword_rdd = self.sc.parallelize(word_count_len_is_stopword_rdd.collect())
        word_insert_sql_rdd = word_count_len_is_stopword_rdd\
            .map(lambda (word, all_num, word_length, is_stopword):\
                                                   self.word_insert_sql_generator(database_name,\
                                                                                  word_table_name,\
                                                                                  word,\
                                                                                  all_num,\
                                                                                  word_length,\
                                                                                  is_stopword)\
                 )
        print word_count_len_is_stopword_rdd.top(1)
        print 1111111111
        logging.info("word_insert_sql_list.count():{0}".format(word_insert_sql_rdd.count()))
        print 2222222222
        logging.info("format(word_insert_sql_rdd.top(1)):{0}".format(word_insert_sql_rdd.top(1)))
        print 3333333333
        cursor = self.con.cursor()
        try:
            word_insert_sql_rdd.map(lambda sql: cursor.execute(sql)).collect()
            self.con.commit()
        except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()
        return



    def word_insert_sql_generator(self, database_name, word_table_name, word, all_num, word_length, is_stopword):
        if self.check_repeat_word_in_database_table(word = word, database_name = database_name, table_name = word_table_name):
            sql = """UPDATE {database_name}.{table_name}
                        SET all_num={all_num}, word_length={word_length}, is_stopword={is_stopword}
                        WHERE word='{word}'"""\
                .format(database_name = database_name,
                        table_name = word_table_name,
                        all_num = all_num,
                        word_length = word_length,
                        is_stopword = is_stopword,
                        word = word)
        else:
            sql = """INSERT INTO {database_name}.{table_name}(word, is_stopword, word_length, all_num)
                      VALUES('{word}', '{is_stopword}', '{word_length}', '{all_num}'))"""\
                .format(database_name = database_name,\
                        table_name = word_table_name,\
                        word = word,\
                        is_stopword = is_stopword,\
                        word_length = word_length,\
                        all_num = all_num)
        return sql
################################### PART3 CLASS TEST ##################################
database_name = "messageDB"
message_table_name = "message_table"
word_table_name = "word_table"
stopword_data_dir = "../data/input/stopword.txt"
pyspark_app_name = "spam-msg_classifier"


WordRecord = UniqueWordSaver(database_name = database_name, stopword_data_dir = stopword_data_dir)
WordRecord.save_stopword_to_database(database_name = database_name, word_table_name = word_table_name)
slash_split_string_1d_tuple = WordRecord.read_split_result_string_from_database(database_name = database_name,
                                                                                message_table_name = message_table_name,
                                                                                pyspark_app_name = pyspark_app_name)
split_string_with_stopword_1d_tuple_rdd = WordRecord.generate_split_string_with_stopword_1d_tuple_rdd(slash_split_string_1d_tuple = slash_split_string_1d_tuple,
                                                                                                      pyspark_app_name = pyspark_app_name)
word_count_rdd = WordRecord.word_count_for_split_string_1d_tuple_rdd(split_string_with_stopword_1d_tuple_rdd = split_string_with_stopword_1d_tuple_rdd)
word_count_len_is_stopword_rdd = WordRecord.compute_len_is_stopword_rdd(word_count_rdd = word_count_rdd)

WordRecord.save_word_count_with_len_rdd_to_database(database_name = database_name,
                                                    word_table_name = word_table_name,
                                                    word_count_len_is_stopword_rdd = word_count_len_is_stopword_rdd)
