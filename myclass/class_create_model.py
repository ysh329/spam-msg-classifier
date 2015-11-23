# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_model.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-11-19 20:18:01
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import time
import MySQLdb
################################### PART2 CLASS && FUNCTION ###########################
class CreateModel(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = CreateModel.__name__))

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

        logging.info("END CLASS {class_name}.".format(class_name = CreateModel.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateModel.__name__, delta_time = self.end))



    def get_id_and_word_index_list_from_database(self, database_name, message_table_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        #sqls.append("SELECT id, word_index_string FROM {database_name}.{table_name}".format(database_name = database_name, table_name = message_table_name))
        sqls.append("SELECT id, word_index_string FROM {database_name}.{table_name} WHERE id < 100".format(database_name = database_name, table_name = message_table_name))
        logging.info("len(sqls):{0}".format(len(sqls)))

        for idx in xrange(len(sqls)):
            sql = sqls[idx]
            try:
                cursor.execute(sql)
                if idx == len(sqls)-1:
                    id_and_word_index_string_tuple = cursor.fetchall()
                    logging.info("len(id_and_word_index_string_tuple):{0}".format(len(id_and_word_index_string_tuple)))
                    logging.info("id_and_word_index_string_tuple[:3]:{0}".format(id_and_word_index_string_tuple[:3]))

            except MySQLdb.Error, e:
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                return

        try:
            id_and_word_index_list_rdd = self.sc.parallelize(id_and_word_index_string_tuple)\
                .map(lambda (id, word_index_string): (int(id), word_index_string.split("///")))
            logging.info("id_and_word_index_list_rdd.persist().is_cached:{0}".format(id_and_word_index_list_rdd.persist().is_cached))
            logging.info("id_and_word_index_list_rdd.count():{0}".format(id_and_word_index_list_rdd.count()))
            logging.info("id_and_word_index_list_rdd.take(3):{0}".format(id_and_word_index_list_rdd.take(3)))
        except Exception as e:
            logging.error(e)

        return id_and_word_index_list_rdd



    def get_id_and_all_num_and_true_pos_num_and_true_neg_num_from_database(self, database_name, word_table_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        sqls.append("SELECT id, all_num, true_pos_num, true_neg_num FROM {database_name}.{table_name}".format(database_name = database_name, table_name = word_table_name))
        logging.info("len(sqls):{0}".format(len(sqls)))

        for idx in xrange(len(sqls)):
            sql = sqls[idx]
            try:
                cursor.execute(sql)
                if idx == len(sqls)-1:
                    id_and_all_num_and_true_pos_num_and_true_neg_num_tuple = cursor.fetchall()
                    logging.info("len(id_and_all_num_and_true_pos_num_and_true_neg_num_tuple):{0}".format(len(id_and_all_num_and_true_pos_num_and_true_neg_num_tuple)))
                    logging.info("id_and_all_num_and_true_pos_num_and_true_neg_num_tuple[:3]:{0}".format(id_and_all_num_and_true_pos_num_and_true_neg_num_tuple[:3]))

            except MySQLdb.Error, e:
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                return

        # sub-function
        def correct_none_and_abnormal_value(id, all_num, true_pos_num, true_neg_num):
            if all_num == None: all_num = 0
            if true_pos_num == None: true_pos_num = 0
            if true_neg_num == None: true_neg_num = 0

            if all_num < true_pos_num + true_neg_num:
                all_num = true_pos_num + true_neg_num
                return (id, all_num, true_pos_num, true_neg_num)

            if all_num > true_pos_num + true_neg_num:
                if true_pos_num == 0:
                    true_neg_num = all_num
                    return (id, all_num, true_pos_num, true_neg_num)
                elif true_neg_num == 0:
                    true_pos_num = all_num
                    return (id, all_num, true_pos_num, true_neg_num)
                else:
                    true_pos_num_and_true_neg_num = true_pos_num + true_neg_num
                    true_pos_num = all_num * (true_pos_num / true_pos_num_and_true_neg_num)
                    true_neg_num = all_num * (true_neg_num / true_pos_num_and_true_neg_num)
                    return (id, all_num, true_pos_num, true_neg_num)
            return (id, all_num, true_pos_num, true_neg_num)


        id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd = self.sc.parallelize(id_and_all_num_and_true_pos_num_and_true_neg_num_tuple)
        id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd = id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd\
            .map(lambda (id, all_num, true_pos_num, true_neg_num):\
                    correct_none_and_abnormal_value(id = int(id),\
                                                    all_num = all_num,\
                                                    true_pos_num = true_pos_num,\
                                                    true_neg_num = true_neg_num)\
                 )
        logging.info("id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd.count():{0}".format(id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd.count()))
        logging.info("id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd.take(3):{0}".format(id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd.take(3)))
        return id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd



    # compute normal message probability of word
    def compute_true_neg_pro_rdd(self, id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd):
        # sub-function
        def compute_true_neg_pro(id, all_num, true_pos_num, true_neg_num):
            if all_num == 0 or true_pos_num == 0 or true_neg_num == 0:
                true_neg_pro = 0.5
            else:
                # version 1
                """
                assumed_neg_prob = true_neg_num / all_num
                weight = 1.0
                initial_pro = 0.5
                true_neg_pro = (weight*assumed_neg_prob + all_num*initial_pro) / (all_num + weight)
                """
                # version 2
                true_neg_pro = 0.5 + 0.5*(float(true_neg_num-true_pos_num) / all_num)
            return (id, true_neg_pro)

        id_and_true_neg_pro_tuple_rdd = id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd\
            .map(lambda (id, all_num, true_pos_num, true_neg_num):\
                    compute_true_neg_pro(id = id,\
                                         all_num = all_num,\
                                         true_pos_num = true_pos_num,\
                                         true_neg_num = true_neg_num)\
                 )
        logging.info("id_and_true_neg_pro_tuple_rdd.count():{0}".format(id_and_true_neg_pro_tuple_rdd.count()))
        logging.info("id_and_true_neg_pro_tuple_rdd.take(3):{0}".format(id_and_true_neg_pro_tuple_rdd.take(3)))
        return id_and_true_neg_pro_tuple_rdd



    def save_true_neg_pro_to_database(self, id_and_true_neg_pro_tuple_rdd, database_name, word_table_name):
        # sub-function
        def true_neg_pro_for_word_sql_generator(id, true_neg_pro, database_name, word_table_name):
            sql = """UPDATE {database_name}.{table_name}
                            SET true_neg_pro={true_neg_pro}
                            WHERE id={id}""".format(database_name = database_name,\
                                                        table_name = word_table_name,\
                                                        true_neg_pro = true_neg_pro,\
                                                        id = id\
                                                    )
            return sql

        true_neg_pro_update_sql_rdd = id_and_true_neg_pro_tuple_rdd.map(lambda (id, true_neg_pro):\
                                                                        true_neg_pro_for_word_sql_generator(id = id,\
                                                                                                            true_neg_pro = true_neg_pro,\
                                                                                                            database_name = database_name,\
                                                                                                            word_table_name = word_table_name)\
                                                                        )
        logging.info("true_neg_pro_update_sql_rdd.persist().is_cached:{0}".format(true_neg_pro_update_sql_rdd.persist().is_cached))
        logging.info("true_neg_pro_update_sql_rdd.count():{0}".format(true_neg_pro_update_sql_rdd.count()))
        logging.info("true_neg_pro_update_sql_rdd.take(3):{0}".format(true_neg_pro_update_sql_rdd.take(3)))

        true_neg_pro_update_sql_rdd_list = true_neg_pro_update_sql_rdd.randomSplit([1,1,1,1, 1,1,1,1])
        cursor = self.con.cursor()
        for rdd_idx in xrange(len(true_neg_pro_update_sql_rdd_list)):
            logging.info("true_neg_pro_update_sql_rdd_list: rdd_idx:{0}".format(rdd_idx))

            true_neg_pro_sql_rdd = true_neg_pro_update_sql_rdd_list[rdd_idx]
            true_neg_pro_sql_list = true_neg_pro_sql_rdd.collect()
            true_neg_pro_sql_list_length = len(true_neg_pro_sql_list)

            success_update = 0
            failure_update = 0
            for sql_idx in xrange(true_neg_pro_sql_list_length):
                if (sql_idx % 10000 == 0 and sql_idx > 9998) or (sql_idx == true_neg_pro_sql_list_length-1):
                    logging.info("==========={sql_idx}th element in true_neg_pro_sql_list of {rdd_idx}th random split rdd===========".format(sql_idx = sql_idx, rdd_idx = rdd_idx))
                    logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx=sql_idx, rate=float(sql_idx+1)/true_neg_pro_sql_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_update / float(success_update + failure_update)))
                    logging.info("success_update:{success}, failure_update:{failure}".format(success = success_update, failure = failure_update))
                sql = true_neg_pro_sql_list[sql_idx]
                try:
                    cursor.execute(sql)
                    self.con.commit()
                    success_update = success_update + 1
                except MySQLdb.Error, e:
                    self.con.rollback()
                    logging.error("error SQL:{0}".format(sql))
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    failure_update = failure_update + 1



    def compute_prob_of_normal_message_given_category_prob(self, database_name, message_table_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        sqls.append("SELECT COUNT(*) FROM {database_name}.{table_name}".format(database_name = database_name, table_name = message_table_name))
        sqls.append("SELECT COUNT(*) FROM {database_name}.{table_name} WHERE true_label=1".format(database_name = database_name, table_name = message_table_name))
        logging.info("len(sqls):{0}".format(len(sqls)))

        for idx in xrange(len(sqls)):
            sql = sqls[idx]
            try:
                cursor.execute(sql)
                if idx == len(sqls)-2:
                    all_train_message_num = int(cursor.fetchall()[0][0])
                    #print "all_train_message_num:", all_train_message_num
                if idx == len(sqls)-1:
                    spam_message_num = int(cursor.fetchall()[0][0])
                    #print "spam_message_num:", spam_message_num
                    if all_train_message_num != 0:
                        prob_of_normal_message_given_normal_category_prob = float(all_train_message_num - spam_message_num) / all_train_message_num
                        #print prob_of_normal_message_given_category_prob
                        logging.info("prob_of_normal_message_given_normal_category_prob：{0}".format(prob_of_normal_message_given_normal_category_prob))
                    else:
                        logging.error("all_train_message_num == 0 is True")

                    logging.info("all_train_message_num:{0}".format(all_train_message_num))
                    logging.info("spam_message_num:{0}".format(spam_message_num))
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()

        return prob_of_normal_message_given_normal_category_prob



    def compute_normal_message_prob_in_train_data_rdd(self, id_and_word_index_list_rdd, id_and_word_and_true_neg_pro_broadcast, prob_of_normal_message_given_normal_category_prob):
        # sub-function
        # improved naive bayes
        def compute_normal_message_prob(word_index_list, id_and_true_neg_pro_tuple_list, prob_of_normal_message_given_normal_category_prob):
            normal_message_prob = prob_of_normal_message_given_normal_category_prob

            for idx in xrange(len(word_index_list)):
                word_index = word_index_list[idx]
                normal_message_prob = normal_message_prob * id_and_word_and_true_neg_pro_tuple_list[word_index-1][1]

            return normal_message_prob

        # word id
        try:
            id_and_word_and_true_neg_pro_tuple_list = id_and_word_and_true_neg_pro_broadcast.value
            id_and_word_and_true_neg_pro_tuple_list = id_and_word_and_true_neg_pro_tuple_list.sort()
            id_and_true_neg_pro_tuple_list = map(lambda (id, word, true_neg_pro):\
                                                     (id, true_neg_pro),\
                                                 id_and_word_and_true_neg_pro_tuple_list\
                                                 )
            logging.info("len(id_and_true_neg_pro_tuple_list):{0}".format(len(id_and_true_neg_pro_tuple_list)))
            logging.info("id_and_true_neg_pro_tuple_list[:3]:{0}".format(id_and_true_neg_pro_tuple_list[:3]))
        except Exception as e:
            logging.error(e)

        # message id
        try:
            id_and_is_spam_prob_rdd = id_and_word_index_list_rdd.map(lambda (message_id, word_index_list):\
                                                                         (message_id,\
                                                                          compute_normal_message_prob(word_index_list = word_index_list,\
                                                                                                      id_and_true_neg_pro_tuple_list = id_and_true_neg_pro_tuple_list,\
                                                                                                      prob_of_normal_message_given_normal_category_prob = prob_of_normal_message_given_normal_category_prob)\
                                                                          )\
                                                                     )\
                .map(lambda (message_id, normal_message_prob):\
                                                               (message_id,\
                                                                1 - normal_message_prob)\
                     )

            logging.info("id_and_is_spam_prob_rdd.persist().is_cached:{0}".format(id_and_is_spam_prob_rdd.persist().is_cached))
            logging.info("id_and_is_spam_prob_rdd.count():{0}".format(id_and_is_spam_prob_rdd.count()))
            logging.info("id_and_is_spam_prob_rdd.take(3):{0}".format(id_and_is_spam_prob_rdd.take(3)))
        except Exception as e:
            logging.error(e)

        return id_and_is_spam_prob_rdd



    def save_message_is_spam_prob_to_database(self, id_and_is_spam_prob_rdd, database_name, message_table_name):
        def is_spam_prob_for_message_sql_generator(id, is_spam_prob, database_name, message_table_name):
            sql = """UPDATE {database_name}.{table_name}
                            SET is_spam_prob={is_spam_prob}
                            WHERE id={id}""".format(database_name = database_name,\
                                                    table_name = message_table_name,\
                                                    is_spam_prob = is_spam_prob,\
                                                    id = id\
                                                    )
            return sql

        # generate update sql
        try:
            id_and_is_spam_prob_update_sql_rdd = id_and_is_spam_prob_rdd\
                .map(lambda (id, is_spam_prob): is_spam_prob_for_message_sql_generator(id = id,\
                                                                                       is_spam_prob = is_spam_prob,\
                                                                                       database_name = database_name,\
                                                                                       message_table_name = message_table_name)\
                     )
            logging.info("id_and_is_spam_prob_update_sql_rdd.persist().is_cached:{0}".format(id_and_is_spam_prob_update_sql_rdd.persist().is_cached))
            logging.info("id_and_is_spam_prob_update_sql_rdd.count():{0}".format(id_and_is_spam_prob_update_sql_rdd.count()))
            logging.info("id_and_is_spam_prob_update_sql_rdd.take(3):{0}".format(id_and_is_spam_prob_update_sql_rdd.take(3)))

            logging.info("id_and_is_spam_prob_rdd.unpersist().is_cached:{0}".format(id_and_is_spam_prob_rdd.unpersist().is_cached))
        except Exception as e:
            logging.error(e)

        id_and_is_spam_prob_update_sql_rdd_list = id_and_is_spam_prob_update_sql_rdd.randomSplit([1,1,1,1, 1,1,1,1])
        cursor = self.con.cursor()
        for rdd_idx in xrange(len(id_and_is_spam_prob_update_sql_rdd_list)):
            logging.info("id_and_is_spam_prob_update_sql_rdd_list: rdd_idx:{0}".format(rdd_idx))

            is_spam_prob_sql_rdd = id_and_is_spam_prob_update_sql_rdd_list[rdd_idx]
            is_spam_prob_sql_list = is_spam_prob_sql_rdd.collect()
            is_spam_prob_sql_list_length = len(is_spam_prob_sql_list)

            success_update = 0
            failure_update = 0
            for sql_idx in xrange(is_spam_prob_sql_list_length):
                if (sql_idx % 10000 == 0 and sql_idx > 9998) or (sql_idx == is_spam_prob_sql_list_length-1):
                    logging.info("==========={sql_idx}th element in is_spam_prob_sql_list of {rdd_idx}th random split rdd===========".format(sql_idx = sql_idx, rdd_idx = rdd_idx))
                    logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx=sql_idx, rate=float(sql_idx+1)/is_spam_prob_sql_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_update / float(success_update + failure_update)))
                    logging.info("success_update:{success}, failure_update:{failure}".format(success = success_update, failure = failure_update))
                sql = is_spam_prob_sql_list[sql_idx]
                try:
                    cursor.execute(sql)
                    self.con.commit()
                    success_update = success_update + 1
                except MySQLdb.Error, e:
                    self.con.rollback()
                    logging.error("error SQL:{0}".format(sql))
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    failure_update = failure_update + 1


    def get_id_and_word_and_true_neg_pro_broadcast_from_database(self, database_name, word_table_name):
        cursor = self.con.cursor()

        sqls = ["USE {database_name}".format(database_name = database_name), "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))
        #sqls.append("SELECT id, word, true_neg_pro FROM {database_name}.{table_name} WHERE id < 100".format(database_name = database_name, table_name = word_table_name))
        sqls.append("SELECT id, word, true_neg_pro FROM {database_name}.{table_name}".format(database_name = database_name, table_name = word_table_name))
        logging.info("len(sqls):{0}".format(len(sqls)))

        for idx in xrange(len(sqls)):
            sql = sqls[idx]
            try:
                cursor.execute(sql)
                if idx == len(sqls)-1:
                    id_and_word_and_true_neg_pro_2d_tuple = cursor.fetchall()
                    logging.info("len(id_and_word_and_true_neg_pro_2d_tuple):{0}".format(len(id_and_word_and_true_neg_pro_2d_tuple)))
                    logging.info("id_and_word_and_true_neg_pro_2d_tuple[:3]:{0}".format(id_and_word_and_true_neg_pro_2d_tuple[:3]))

            except MySQLdb.Error, e:
                logging.error("error SQL:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                return

        # normalization
        id_and_word_and_true_neg_pro_tuple_list = map(lambda (id, word, true_neg_pro): (int(id), word, float(true_neg_pro)), id_and_word_and_true_neg_pro_2d_tuple)

        try:
            id_and_word_and_true_neg_pro_broadcast = self.sc.broadcast(id_and_word_and_true_neg_pro_tuple_list)
            logging.info("len(id_and_word_and_true_neg_pro_broadcast.value):{0}".format(len(id_and_word_and_true_neg_pro_broadcast.value)))
            logging.info("id_and_word_and_true_neg_pro_broadcast.value[:3]:{0}".format(id_and_word_and_true_neg_pro_broadcast.value[:3]))
        except Exception as e:
            logging.error(e)

        return id_and_word_and_true_neg_pro_broadcast

################################### PART3 CLASS TEST ##################################

# Initialization Parameters
database_name = "messageDB"
message_table_name = "message_table"
word_table_name = "word_table"
from pyspark import SparkContext
pyspark_sc = SparkContext("")

Model = CreateModel(database_name = database_name, pyspark_sc = pyspark_sc)

id_and_word_index_list_rdd = Model.get_id_and_word_index_list_from_database(database_name = database_name,\
                                                                            message_table_name = message_table_name)
"""
id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd = Model\
    .get_id_and_all_num_and_true_pos_num_and_true_neg_num_from_database(database_name = database_name,\
                                                                        word_table_name = word_table_name)
id_and_true_neg_pro_tuple_rdd = Model\
    .compute_true_neg_pro_rdd(id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd = id_and_all_num_and_true_pos_num_and_true_neg_num_tuple_rdd)

Model.save_true_neg_pro_to_database(id_and_true_neg_pro_tuple_rdd = id_and_true_neg_pro_tuple_rdd,\
                                    database_name = database_name,\
                                    word_table_name = word_table_name)
"""
Model.compute_prob_of_normal_message_given_category_prob(database_name = database_name,\
                                                         message_table_name = message_table_name)


id_and_word_and_true_neg_pro_broadcast = Model\
    .get_id_and_word_and_true_neg_pro_broadcast_from_database(database_name = database_name,\
                                                              word_table_name = word_table_name)

id_and_is_spam_prob_rdd = Model\
    .compute_normal_message_prob_in_train_data_rdd(id_and_word_index_list_rdd = id_and_word_index_list_rdd,\
                                                   id_and_word_and_true_neg_pro_broadcast = id_and_word_and_true_neg_pro_broadcast)

Model.save_message_is_spam_prob_to_database(id_and_is_spam_prob_rdd = id_and_is_spam_prob_rdd,\
                                            database_name = database_name,\
                                            message_table_name = message_table_name)