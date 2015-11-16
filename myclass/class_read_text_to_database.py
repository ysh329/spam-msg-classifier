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
from pyspark import SparkContext, SparkConf, StorageLevel
################################### PART2 CLASS && FUNCTION ###########################
class ReadText2DB(object):

    def __init__(self, database_name, train_data_dir, stopword_data_dir, pyspark_app_name):
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
            conf = SparkConf().setAppName(pyspark_app_name)
            conf = conf.setMaster("local[8]")
            self.sc = SparkContext(conf = conf)
            logging.info("Start pyspark successfully.")
        except Exception as e:
            logging.error("Fail in starting pyspark.")
            logging.error(e)



    def __del__(self):
        self.train_f.close()
        self.stopword_f.close()
        self.con.close()

        logging.info("Success in closing file.")
        logging.info("Success in quiting MySQL.")
        logging.info("END.")

        self.end = time.clock()
        logging.info("The function run time is : %.03f seconds" % (self.end - self.start))



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
        #message_insert_sql_rdd.cache().is_cached
        #logging.info("message_insert_sql_rdd.cache().is_cached:{0}".format(message_insert_sql_rdd.cache().is_cached))
        #message_insert_sql_rdd.checkpoint("")
        return message_insert_sql_rdd



    def save_train_data_to_database(self, message_insert_sql_rdd):
        
        message_insert_sql_id_rdd = self.sc.parallelize(xrange(message_insert_sql_rdd.count()))
        #　拼接两个ｒｄｄ，之后filter出８个rdd，依次执行
        #message_insert_sql_list = message_insert_sql_rdd.filter(lambda tup:)collect()
        logging.info("len(message_insert_sql_list):{0}".format(len(message_insert_sql_list)))
        logging.info("message_insert_sql_list[0]:{0}".format(message_insert_sql_list[0]))

        success_insert = 0
        failure_insert = 0

        cursor = self.con.cursor()
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
        """
        def sql_executor(self, cursor, sql):
            try:
                cursor.execute(sql)
                self.con.commit()
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                logging.error("error SQL:{0}".format(sql))


        cursor = self.con.cursor()
        #prepare_execute_message_insert_sql_rdd = message_insert_sql_rdd.map(lambda message_insert_sql: cursor.execute(message_insert_sql))
        prepare_execute_message_insert_sql_rdd = message_insert_sql_rdd.map(lambda message_insert_sql: sql_executor(self=self,\
                                                                                                                    cursor = cursor,\
                                                                                                                    sql = message_insert_sql)\
                                                                            )

        try:
            prepare_execute_message_insert_sql_rdd.count()
            self.con.commit()
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()
        """



    # (id, is_train, true_label, word_num, content, split_result_string, split_result_num, split_result_clean_num)
    def read_text_into_meta_data(self, data_set):
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
            logging.error("Can't read training data set.\nTermination.")
            return

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
        content_list = map(lambda record: record[2].strip().lower(), train_data_triple_2d_list)

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
            content = content_list[id_idx].replace("'", '"').replace("\\", "")
            split_result_string = split_result_string_list[id_idx].encode('utf8').replace("'", '"').replace("\\", "")
            split_result_num = split_result_num_list[id_idx]

            sqls.append("""INSERT INTO %s.%s(id, is_train, true_label, word_num, content, split_result_string, split_result_num) VALUES (%s, %s, %s, %s, '%s', '%s', %s)""" % (database_name, table_name, id, is_train, true_label, word_num, content, split_result_string, split_result_num))

        logging.info("len(sqls): %s." % len(sqls))
        logging.info("sqls[10]: %s." % sqls[10])
        logging.info("type(sqls[10]): %s." % type(sqls[10]))
        logging.info("sqls[len(sqls)-1]: %s." % sqls[len(sqls)-1])

        '''
        try:
            map(lambda sql: cursor.execute(sql), sqls)
            self.con.commit()
        except Exception as e:
            logging.error(e)
            logging.error(sql)
        '''
        success_insert = 0
        failure_insert = 0
        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            if sql_idx % 1000 == 0:
                logging.info("sql_idx: %s." % sql_idx)

            try:
                cursor.execute(sql)
                self.con.commit()
                success_insert = success_insert + 1
            except MySQLdb.Error, e:
                self.con.rollback()
                failure_insert = failure_insert + 1
                logging.error("Error SQL[sql_idx: %s]: %s." % (sql_idx, sql))
                logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))

        logging.info("success_insert: %s." % success_insert)
        logging.info("failure_insert: %s." % failure_insert)
        logging.info("summation insert: %s." % (success_insert + failure_insert))
        cursor.close()




    """
    def read_text_into_clean_data(self, split_result_2d_list):

        split_result_1d_list = list(set(sum(split_result_2d_list, [])))
        logging.info("")
        logging.info("len(split_result_1d_list): %s." % len(split_result_1d_list))
        logging.info("type(split_result_1d_list): %s." % type(split_result_1d_list))
        logging.info("split_result_1d_list[0]: %s." % split_result_1d_list[0])
        logging.info("type(split_result_1d_list[0]): %s." % type(split_result_1d_list[0]))
        logging.info("split_result_1d_list[len(split_result_1d_list)-1]: %s." % split_result_1d_list[len(split_result_1d_list)-1])
        logging.info("type(split_result_1d_list[len(split_result_1d_list)-1]): %s." % type(split_result_1d_list[len(split_result_1d_list)-1]))
    """

################################### PART3 CLASS TEST ##################################

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
                     pyspark_app_name = pyspark_app_name)
"""
#id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list, split_result_2d_list = Reader.read_text_into_meta_data()
#Reader.save_meta_data_to_database(database_name, table_name_list[0], id_list, is_train_list, true_label_list, word_num_list, content_list, split_result_string_list, split_result_num_list)

#Reader.read_text_into_clean_data(split_result_2d_list)
Reader.save_stopword_to_database(database_name = database_name,
                             table_name_list = table_name_list)
"""

cleaned_and_processed_train_data_rdd = Reader.read_train_data(train_data_dir = train_data_dir,
                                                              stopword_data_dir = stopword_data_dir)
message_insert_sql_rdd = Reader.message_insert_sql_generator(database_name = database_name,
                                    message_table_name = message_table_name,
                                    cleaned_and_processed_train_data_rdd = cleaned_and_processed_train_data_rdd)
Reader.save_train_data_to_database(message_insert_sql_rdd = message_insert_sql_rdd)
