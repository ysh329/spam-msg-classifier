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
from operator import add
################################### PART2 CLASS && FUNCTION ###########################
class String2WordVec(object):
    def __init__(self, database_name, pyspark_sc):
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
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = String2WordVec.__name__, delta_time = self.end))



    def get_message_rdd_from_database(self, database_name, message_table_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        #sqls.append("SELECT id, true_label, split_result_clean_string FROM {database_name}.{table_name} WHERE id < 1000".format(database_name = database_name, table_name = message_table_name))
        sqls.append("SELECT id, true_label, split_result_clean_string FROM {database_name}.{table_name}".format(database_name = database_name, table_name = message_table_name))
        logging.info("len(sqls):{0}".format(len(sqls)))
        for idx in xrange(len(sqls)):
            sql = sqls[idx]
            try:
                cursor.execute(sql)
                if idx == len(sqls)-1:
                    id_and_true_label_and_clean_string_2d_tuple = cursor.fetchall()
                    logging.info("len(id_and_true_label_and_clean_string_2d_tuple):{0}".format(len(id_and_true_label_and_clean_string_2d_tuple)))
                    logging.info("id_and_true_label_and_clean_string_2d_tuple[0]:{0}".format(id_and_true_label_and_clean_string_2d_tuple[0]))
                    logging.info("id_and_true_label_and_clean_string_2d_tuple[:3]:{0}".format(id_and_true_label_and_clean_string_2d_tuple[:3]))

                    id_and_true_label_and_clean_string_list_message_rdd = self.sc.parallelize(id_and_true_label_and_clean_string_2d_tuple).map(lambda (id,\
                                                                                                                                                          true_label,\
                                                                                                                                                          split_result_clean_string):\
                                                                                                                                                   (int(id),\
                                                                                                                                                    int(true_label),\
                                                                                                                                                    split_result_clean_string.split("///")\
                                                                                                                                                    )\
                                                                                                                                               )
                    logging.info("id_and_true_label_and_clean_string_list_message_rdd.count():{0}".format(id_and_true_label_and_clean_string_list_message_rdd.count()))
                    logging.info("id_and_true_label_and_clean_string_list_message_rdd.take(1):{0}".format(id_and_true_label_and_clean_string_list_message_rdd.take(1)))
            except MySQLdb.Error, e:
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                return
        cursor.close()

        # get spam_message_clean_string_list_rdd, normal_message_clean_string_list_rdd

        try:
            spam_message_clean_string_list_rdd = id_and_true_label_and_clean_string_list_message_rdd\
                .filter(lambda (id, true_label, clean_string_list): true_label == 1)\
                .map(lambda (id, true_label, clean_string_list): clean_string_list)
            logging.info("spam_message_clean_string_list_rdd.persist().is_cached:{0}".format(spam_message_clean_string_list_rdd.persist().is_cached))
            logging.info("spam_message_clean_string_list_rdd.count():{0}".format(spam_message_clean_string_list_rdd.count()))
            logging.info("spam_message_clean_string_list_rdd.take(3):{0}".format(spam_message_clean_string_list_rdd.take(3)))

            normal_message_clean_string_list_rdd = id_and_true_label_and_clean_string_list_message_rdd\
                .filter(lambda (id, true_label, clean_string_list): true_label == 0)\
                .map(lambda (id, true_label, clean_string_list): clean_string_list)
            logging.info("normal_message_clean_string_list_rdd.persist().is_cached:{0}".format(normal_message_clean_string_list_rdd.persist().is_cached))
            logging.info("normal_message_clean_string_list_rdd.count():{0}".format(normal_message_clean_string_list_rdd.count()))
            logging.info("normal_message_clean_string_list_rdd.take(3):{0}".format(normal_message_clean_string_list_rdd.take(3)))
        except Exception as e:
            logging.error(e)

        return spam_message_clean_string_list_rdd, normal_message_clean_string_list_rdd



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
                    logging.info("id_and_word_rdd.persist().is_cached:{0}".format(id_and_word_rdd.persist().is_cached))
                    logging.info("id_and_word_rdd.count():{0}".format(id_and_word_rdd.count()))
                    logging.info("id_and_word_rdd.take(1):{0}".format(id_and_word_rdd.take(1)))
            except MySQLdb.Error, e:
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()

        return id_and_word_rdd



    def string_list_rdd_to_dict_rdd(self, spam_message_clean_string_list_rdd, normal_message_clean_string_list_rdd):
        # sub-function
        def string_list_to_dict(string_list):
            word_dict = dict()
            string_list_set_iterator = iter(set(string_list))
            for word_idx in xrange(string_list_set_iterator.__length_hint__()):
                word = string_list_set_iterator.next()
                word_dict[word] = 0

            string_list_iterator = iter(string_list)
            for word_idx in xrange(len(string_list)):
                word = string_list_iterator.next()
                word_dict[word] = word_dict[word] + 1
            return word_dict

        try:
            spam_message_clean_string_dict_rdd = spam_message_clean_string_list_rdd.map(string_list_to_dict)
            logging.info("spam_message_clean_string_dict_rdd.persist().is_cached:{0}".format(spam_message_clean_string_dict_rdd.persist().is_cached))
            logging.info("spam_message_clean_string_dict_rdd.count():{0}".format(spam_message_clean_string_dict_rdd.count()))
            logging.info("spam_message_clean_string_dict_rdd.take(2)".format(spam_message_clean_string_dict_rdd.take(2)))

            normal_message_clean_string_dict_rdd = normal_message_clean_string_list_rdd.map(string_list_to_dict)
            logging.info("normal_message_clean_string_dict_rdd.persist().is_cached:{0}".format(normal_message_clean_string_dict_rdd.persist().is_cached))
            logging.info("normal_message_clean_string_dict_rdd.count()):{0}".format(normal_message_clean_string_dict_rdd.count()))
            logging.info("normal_message_clean_string_dict_rdd.take(2):{0}".format(normal_message_clean_string_dict_rdd.take(2)))
        except Exception as e:
            logging.error(e)
            return

        return spam_message_clean_string_dict_rdd, normal_message_clean_string_dict_rdd



    def string_to_word_vector(self, id_and_clean_string_list_message_rdd, id_and_word_rdd):
        pass



    def string_to_index_vector(self, word_string):
        pass



    def index_vector_to_word_vector(self, index_vector_list):
        pass


    def word_count_for_spam_and_normal_message(self, spam_message_clean_string_list_rdd, normal_message_clean_string_list_rdd):
        # word list
        spam_message_word_list = spam_message_clean_string_list_rdd.reduce(lambda l1, l2: l1 + l2)
        logging.info("len(spam_message_word_list):{0}".format(len(spam_message_word_list)))
        logging.info("type(spam_message_word_list):{0}".format(type(spam_message_word_list)))
        logging.info("spam_message_word_list[:10]:{0}".format(spam_message_word_list[:10]))

        normal_message_word_list = normal_message_clean_string_list_rdd.reduce(lambda l1, l2: l1 + l2)
        logging.info("len(normal_message_word_list):{0}".format(len(normal_message_word_list)))
        logging.info("type(normal_message_word_list):{0}".format(type(normal_message_word_list)))
        logging.info("normal_message_word_list[:10]:{0}".format(normal_message_word_list[:10]))

        # word count
        spam_message_word_count_rdd = self.sc.parallelize(spam_message_word_list).map(lambda word: (word, 1)).reduceByKey(add)
        logging.info("spam_message_word_count_rdd.count():{0}".format(spam_message_word_count_rdd.count()))
        logging.info("spam_message_word_count_rdd.persist().is_cached:{0}".format(spam_message_word_count_rdd.persist().is_cached))
        logging.info("spam_message_word_count_rdd.take(3):{0}".format(spam_message_word_count_rdd.take(3)))

        normal_message_word_count_rdd = self.sc.parallelize(normal_message_word_list).map(lambda word: (word, 1)).reduceByKey(add)
        logging.info("normal_message_word_count_rdd.count():{0}".format(normal_message_word_count_rdd.count()))
        logging.info("normal_message_word_count_rdd.persist().is_cached:{0}".format(normal_message_word_count_rdd.persist().is_cached))
        logging.info("normal_message_word_count_rdd.take(3):{0}".format(normal_message_word_count_rdd.take(3)))

        return spam_message_word_count_rdd, normal_message_word_count_rdd



    def save_true_pos_and_neg_num_to_database(self, database_name, word_table_name, spam_message_word_count_rdd, normal_message_word_count_rdd):
        # sub-function
        def word_count_for_spam_and_normal_message_sql_generator(spam_or_normal, word_count_tuple, database_name, word_table_name):
            word = word_count_tuple[0]
            if spam_or_normal == "spam":
                # spam word ==> true_pos_num
                true_pos_num = word_count_tuple[1]
                sql = """UPDATE {database_name}.{table_name}
                            SET true_pos_num={true_pos_num}
                            WHERE word='{word}'""".format(database_name = database_name,\
                                                        table_name = word_table_name,\
                                                        true_pos_num = true_pos_num,\
                                                        word = word.encode("utf8"))
            else:
                # normal word ==> true_neg_num
                true_neg_num = word_count_tuple[1]
                sql = """UPDATE {database_name}.{table_name}
                            SET true_neg_num={true_neg_num}
                            WHERE word='{word}'""".format(database_name = database_name,\
                                                        table_name = word_table_name,\
                                                        true_neg_num = true_neg_num,\
                                                        word = word.encode("utf8"))
            return sql


        # generate word count sql for spam and normal message
        spam_message_word_count_sql_rdd = spam_message_word_count_rdd\
            .map(lambda word_count_tuple: word_count_for_spam_and_normal_message_sql_generator(spam_or_normal = "spam",\
                                                                                               word_count_tuple = word_count_tuple,\
                                                                                               database_name = database_name,\
                                                                                               word_table_name = word_table_name)\
                 )
        logging.info("spam_message_word_count_sql_rdd.count():{0}".format(spam_message_word_count_sql_rdd.count()))
        logging.info("spam_message_word_count_sql_rdd.persist().is_cached:{0}".format(spam_message_word_count_sql_rdd.persist().is_cached))
        logging.info("spam_message_word_count_sql_rdd.take(3):{0}".format(spam_message_word_count_sql_rdd.take(3)))

        normal_message_word_count_sql_rdd = normal_message_word_count_rdd\
            .map(lambda word_count_tuple: word_count_for_spam_and_normal_message_sql_generator(spam_or_normal = "normal",\
                                                                                               word_count_tuple = word_count_tuple,\
                                                                                               database_name = database_name,\
                                                                                               word_table_name = word_table_name)\
                 )
        logging.info("normal_message_word_count_sql_rdd.count():{0}".format(normal_message_word_count_sql_rdd.count()))
        logging.info("normal_message_word_count_sql_rdd.persist().is_cached:{0}".format(normal_message_word_count_sql_rdd.persist().is_cached))
        logging.info("spam_message_word_count_sql_rdd.take(3):{0}".format(spam_message_word_count_sql_rdd.take(3)))


        # update normal message for true_pos_num
        normal_message_word_count_sql_rdd_random_split_list = normal_message_word_count_sql_rdd.randomSplit([1,1,1,1, 1,1,1,1])
        cursor = self.con.cursor()
        for rdd_idx in xrange(len(normal_message_word_count_sql_rdd_random_split_list)):
            logging.info("normal_message_word_count_sql_rdd_random_split_list: rdd_idx:{0}".format(rdd_idx))

            normal_message_sql_rdd = normal_message_word_count_sql_rdd_random_split_list[rdd_idx]
            normal_message_sql_list = normal_message_sql_rdd.collect()
            normal_message_sql_list_length = len(normal_message_sql_list)

            success_update = 0
            failure_update = 0
            for sql_idx in xrange(normal_message_sql_list_length):
                if (sql_idx % 10000 == 0 and sql_idx > 9998) or (sql_idx == normal_message_sql_list_length-1):
                    logging.info("==========={sql_idx}th element in normal_message_sql_list of {rdd_idx}th random split rdd===========".format(sql_idx = sql_idx, rdd_idx = rdd_idx))
                    logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx=sql_idx, rate=float(sql_idx+1)/normal_message_sql_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_update / float(success_update + failure_update)))
                    logging.info("success_update:{success}, failure_update:{failure}".format(success = success_update, failure = failure_update))
                sql = normal_message_sql_list[sql_idx]
                try:
                    cursor.execute(sql)
                    self.con.commit()
                    success_update = success_update + 1
                except MySQLdb.Error, e:
                    self.con.rollback()
                    logging.error("error SQL:{0}".format(sql))
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    failure_update = failure_update + 1

        # update spam message for true_neg_num
        success_update = 0
        failure_update = 0
        spam_message_sql_list = spam_message_word_count_sql_rdd.collect()
        spam_message_sql_list_length = len(spam_message_sql_list)
        for sql_idx in xrange(spam_message_sql_list_length):
            if (sql_idx % 10000 == 0 and sql_idx > 9998) or (sql_idx == spam_message_sql_list_length-1):
                logging.info("==========={0}th element in spam_message_sql_list===========".format(sql_idx))
                logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx=sql_idx, rate=float(sql_idx+1)/spam_message_sql_list_length))
                logging.info("success_rate:{success_rate}".format(success_rate = success_update / float(success_update + failure_update)))
                logging.info("success_update:{success}, failure_update:{failure}".format(success = success_update, failure = failure_update))

            sql = spam_message_sql_list[sql_idx]
            try:
                cursor.execute(sql)
                self.con.commit()
                success_update = success_update + 1
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                failure_update = failure_update + 1
        cursor.close()

################################### PART3 CLASS TEST ##################################
"""
# Initialization Parameters
database_name = "messageDB"
message_table_name = "message_table"
word_table_name = "word_table"
from pyspark import SparkContext
pyspark_sc = SparkContext("")

Word2Vec = String2WordVec(database_name = database_name, pyspark_sc = pyspark_sc)
spam_message_clean_string_list_rdd, normal_message_clean_string_list_rdd = Word2Vec.get_message_rdd_from_database(database_name = database_name, message_table_name = message_table_name)
#spam_message_clean_string_dict_rdd, normal_message_clean_string_dict_rdd = Word2Vec.string_list_rdd_to_dict_rdd(spam_message_clean_string_list_rdd = spam_message_clean_string_list_rdd, normal_message_clean_string_list_rdd = normal_message_clean_string_list_rdd)

spam_message_word_count_rdd, normal_message_word_count_rdd = Word2Vec.word_count_for_spam_and_normal_message(spam_message_clean_string_list_rdd =  spam_message_clean_string_list_rdd, normal_message_clean_string_list_rdd = normal_message_clean_string_list_rdd)
Word2Vec.save_true_pos_and_neg_num_to_database(database_name = database_name, word_table_name = word_table_name, spam_message_word_count_rdd = spam_message_word_count_rdd, normal_message_word_count_rdd = normal_message_word_count_rdd)
"""