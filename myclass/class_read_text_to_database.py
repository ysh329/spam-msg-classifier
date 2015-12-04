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

    def __init__(self, database_name, train_data_dir, stopword_data_dir, pyspark_sc):
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

        # Configure Spark
        try:
            self.sc = pyspark_sc
            logging.info("Start pyspark successfully.")
        except Exception as e:
            logging.error("Fail in starting pyspark.")
            logging.error(e)



    def __del__(self):
        try:
            self.train_f.close()
            logging.info("Success in closing training data file.")
        except Exception as e:
            logging.error("Fail in closing training data file.")
            logging.error(e)

        try:
            self.stopword_f.close()
            logging.info("Success in closing stop word data file.")
        except Exception as e:
            logging.error("Fail in closing stop word data file.")
            logging.error(e)

        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in quiting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        logging.info("END CLASS {class_name}.".format(class_name = ReadText2DB.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = ReadText2DB.__name__, delta_time = self.end))



    def read_train_data(self, train_data_dir, stopword_data_dir):
        # sub function
        def compute_split_result_clean_string(split_result_string_list, stopword_list):
            none_stopword_bool_flag_list = map(lambda word: word not in stopword_list, split_result_string_list)
            stopword_num = none_stopword_bool_flag_list.count(False)
            split_result_clean_list_without_filter = map(lambda no_stopword_flag, word: no_stopword_flag * word, none_stopword_bool_flag_list, split_result_string_list)
            split_result_clean_list = filter(lambda word: len(word) > 0, split_result_clean_list_without_filter)
            split_result_clean_string = "///".join(split_result_clean_list)
            split_result_clean_num = len(split_result_clean_list)
            return split_result_clean_string, split_result_clean_num, stopword_num


        train_data_rdd = self.sc.textFile(name = train_data_dir, minPartitions = 1, use_unicode = True)
        #train_data_rdd = self.sc.parallelize(train_data_rdd.take(100), 2)
        train_data_triple_rdd = train_data_rdd.map(lambda line: line.split("\t"))\
            .map(lambda (id, true_label, content): (id.strip(),\
                                                    true_label.strip(),\
                                                    content.strip().replace("'", '"').replace("\\", "")\
                                                   )\
                 )
        #train_data_rdd.persist(storageLevel = StorageLevel.MEMORY_ONLY_SER)

        # compute final rdd
        # which contains 9 elements
        # (id, is_train, true_label, content, word_num, split_result_string, split_result_num, split_result_clean_string, split_result_clean_num, stopword_num)
        with open(stopword_data_dir, "r")\
                as stopword_file:
            stopword_list = map(lambda line: line.strip().decode("utf8"), stopword_file.readlines())
        is_train = 1
        cleaned_and_processed_train_data_rdd = train_data_triple_rdd.map(lambda (id,\
                                                                                true_label,\
                                                                                content): (id,\
                                                                                           is_train,\
                                                                                           true_label,\
                                                                                           content,\
                                                                                           len(content),\
                                                                                           "///".join(list(jieba.cut(content.lower()))),\
                                                                                           len(list(jieba.cut(content.lower()))),\
                                                                                           compute_split_result_clean_string(list(jieba.cut(content.lower())), stopword_list),\
                                                                                           )\
                                                                         )\
                                                                          .map(lambda (id,\
                                                                                       is_train,\
                                                                                       true_label,\
                                                                                       content,\
                                                                                       word_num,\
                                                                                       split_result_string,\
                                                                                       split_result_num,\
                                                                                       (split_result_clean_string,
                                                                                        split_result_clean_num,
                                                                                        stopword_num),\
                                                                                       ): (id,\
                                                                                           is_train,\
                                                                                           true_label,\
                                                                                           content,\
                                                                                           word_num,\
                                                                                           split_result_string,\
                                                                                           split_result_num,\
                                                                                           split_result_clean_string,\
                                                                                           split_result_clean_num,\
                                                                                           stopword_num,\
                                                                                           )\
                                                                               )
        logging.info("cleaned_and_processed_train_data_rdd.count():{0}".format(cleaned_and_processed_train_data_rdd.count()))
        logging.info("cleaned_and_processed_train_data_rdd.take(1):{0}".format(cleaned_and_processed_train_data_rdd.take(1)))
        logging.info("len(cleaned_and_processed_train_data_rdd.take(1)):{0}".format(len(cleaned_and_processed_train_data_rdd.take(1))))
        return cleaned_and_processed_train_data_rdd



    def message_insert_sql_generator(self, database_name, message_table_name, cleaned_and_processed_train_data_rdd):
        message_insert_sql_rdd = cleaned_and_processed_train_data_rdd.map(lambda (id,\
                                                                                  is_train,\
                                                                                  true_label,\
                                                                                  content,\
                                                                                  word_num,\
                                                                                  split_result_string,\
                                                                                  split_result_num,\
                                                                                  split_result_clean_string,\
                                                                                  split_result_clean_num,\
                                                                                  stopword_num,\
                                                                                 ):\
                                                                              """INSERT INTO {database_name}.{message_table_name}
                                                                                 (id,
                                                                                  is_train,
                                                                                  true_label,
                                                                                  content,
                                                                                  word_num,
                                                                                  split_result_string,
                                                                                  split_result_num,
                                                                                  split_result_clean_string,
                                                                                  split_result_clean_num,
                                                                                  stopword_num)
                                                                                  VALUES
                                                                                  ({id},
                                                                                   {is_train},
                                                                                   {true_label},
                                                                                   '{content}',
                                                                                   {word_num},
                                                                                   '{split_result_string}',
                                                                                   {split_result_num},
                                                                                   '{split_result_clean_string}',
                                                                                   {split_result_clean_num},
                                                                                   {stopword_num})"""\
                                                                          .format(database_name = database_name,\
                                                                                  message_table_name = message_table_name,\
                                                                                  id = id,\
                                                                                  is_train = is_train,\
                                                                                  true_label = true_label,\
                                                                                  content = content.encode('utf8'),\
                                                                                  word_num = word_num,\
                                                                                  split_result_string = split_result_string.encode('utf8'),\
                                                                                  split_result_num = split_result_num,\
                                                                                  split_result_clean_string = split_result_clean_string.encode('utf8'),\
                                                                                  split_result_clean_num = split_result_clean_num,\
                                                                                  stopword_num = stopword_num,\
                                                                                 ).replace("\n", "")\
                                                                          )
        logging.info("message_insert_sql_rdd.count():{0}".format(message_insert_sql_rdd.count()))
        logging.info("message_insert_sql_rdd.take(1):{0}".format(message_insert_sql_rdd.take(1)))
        logging.info("message_insert_sql_rdd.take(3):{0}".format(message_insert_sql_rdd.take(3)))
        logging.info("message_insert_sql_rdd.cache().is_cached:{0}".format(message_insert_sql_rdd.cache().is_cached))
        #message_insert_sql_rdd.checkpoint("")
        return message_insert_sql_rdd



    def save_train_data_to_database(self, message_insert_sql_rdd):

        #message_insert_sql_id_rdd = self.sc.parallelize(xrange(message_insert_sql_rdd.count()))
        #　拼接两个ｒｄｄ，之后filter出８个rdd，依次执行

        #message_insert_sql_list = message_insert_sql_rdd.collect()
        #logging.info("len(message_insert_sql_list):{0}".format(len(message_insert_sql_list)))
        #logging.info("message_insert_sql_list[0]:{0}".format(message_insert_sql_list[0]))

        message_insert_sql_rdd.cache()
        message_insert_sql_rdd_list= message_insert_sql_rdd.randomSplit([1, 1, 1, 1, 1, 1, 1, 1])

        cursor = self.con.cursor()
        for rdd_idx in xrange(len(message_insert_sql_rdd_list)):
            message_insert_sql_list = message_insert_sql_rdd_list[rdd_idx].collect()# + rdd2.collect() + rdd3.collect() + rdd4.collect()
            logging.info("len(message_insert_sql_list):{0}".format(len(message_insert_sql_list)))

            success_insert = 0
            failure_insert = 0

            for idx in xrange(len(message_insert_sql_list)):
                sql = message_insert_sql_list[idx]
                if (idx % 10000 == 0 and idx > 9998) or (idx == len(message_insert_sql_list) -1):
                    logging.info("==========={0}th element in message_insert_sql_list===========".format(idx))
                    logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx = idx, rate = float(idx+1)/ len(message_insert_sql_list)))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_insert / float(success_insert + failure_insert)))
                    logging.info("success_insert:{success}, failure_insert:{failure}".format(success = success_insert, failure = failure_insert))

                try:
                    cursor.execute(sql)
                    self.con.commit()
                    success_insert = success_insert + 1
                except MySQLdb.Error, e:
                    self.con.rollback()
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    logging.error("error SQL:{0}".format(sql))
                    failure_insert = failure_insert + 1
        cursor.close()
################################### PART3 CLASS TEST ##################################
"""
# initial parameters
database_name = "messageDB"
table_name_list = ["message_table", "word_table"]
train_data_dir = "../data/input/train_set_80W.txt"
stopword_data_dir = "../data/input/stopword.txt"
pyspark_app_name = "spam-msg-classifier"

word_table_name = "word_table"
message_table_name = "message_table"


Reader = ReadText2DB(database_name = database_name,
                     train_data_dir = train_data_dir,
                     stopword_data_dir = stopword_data_dir,
                     pyspark_sc = pyspark_sc)

#id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list, split_result_2d_list = Reader.read_text_into_meta_data()
#Reader.save_meta_data_to_database(database_name, table_name_list[0], id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list)

#Reader.read_text_into_clean_data(split_result_2d_list)
#Reader.save_stopword_to_database(database_name = database_name,
#                             table_name_list = table_name_list)


cleaned_and_processed_train_data_rdd = Reader.read_train_data(train_data_dir = train_data_dir,
                                                              stopword_data_dir = stopword_data_dir)
message_insert_sql_rdd = Reader.message_insert_sql_generator(database_name = database_name,
                                    message_table_name = message_table_name,
                                    cleaned_and_processed_train_data_rdd = cleaned_and_processed_train_data_rdd)
Reader.save_train_data_to_database(message_insert_sql_rdd = message_insert_sql_rdd)
"""