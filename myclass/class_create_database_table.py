__author__ = 'yuens'


import MySQLdb
import logging
import time

class createDatabaseTable(object):

    def __init__(self):
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

        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))



    def __del__(self):
        self.con.close()

        logging.info("Success in quiting MySQL.")
        logging.info("END.")

        self.end = time.clock()
        logging.info("The function run time is : %.03f seconds" % (self.end - self.start))



    def create_database(self, database_name):
        logging.info("database name:%s" % database_name)

        cursor = self.con.cursor()
        sqls = ['SET NAMES UTF8', 'SELECT VERSION()', 'CREATE DATABASE %s' % database_name]
        try:
            for sql_idx in xrange(len(sqls)):
                sql = sqls[sql_idx]
                cursor.execute(sql)
                if sql_idx == 1:
                    result = cursor.fetchall()[0]
                    mysql_version = result[0]
                    logging.info("MySQL VERSION: %s" % mysql_version)
            self.con.commit()
            logging.info("Success in creating database %s." % database_name)
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in creating database %s." % database_name)
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
        finally:
            cursor.close()



    def create_table(self, database_name, table_name_list):
        logging.info("")

        cursor = self.con.cursor()
        sqls = ['USE %s' % database_name, 'SET NAMES UTF8']

        sqls.append("ALTER DATABASE %s DEFAULT CHARACTER SET 'utf8'" % database_name)

        # table_name_list[0]: message_table
        sqls.append("""CREATE TABLE IF NOT EXISTS %s(
                                id INT(11) AUTO_INCREMENT PRIMARY KEY,
                                istrain INT(11),
                                true_label INT(11),
                                predicted_lable INT(11),
                                isspam_prob FLOAT,
                                word_num INT(11),
                                keyword1 TEXT,
                                keyword2 TEXT,
                                keyword3 TEXT,
                                content TEXT NOT NULL,
                                splited_result_string TEXT,
                                word_index_string TEXT,
                                word_vector_string TEXT)""" % table_name_list[0])
        sqls.append("CREATE INDEX id_idx ON %s(id)" % table_name_list[0])

        # table_name_list[1]: word_table
        sqls.append("""CREATE TABLE IF NOT EXISTS %s(
                                id INT(11) AUTO_INCREMENT PRIMARY KEY,
                                word INT(11),
                                word_length INT(11),
                                topic TEXT,
                                true_pos_num INT(11),
                                true_neg_num INT(11),
                                all_num INT(11),
                                true_neg_pro FLOAT,
                                predicted_pos_num INT(11),
                                predicted_neg_num INT(11))""" % table_name_list[1])
        sqls.append("CREATE INDEX id_idx ON %s(id)" % table_name_list[1])

        try:
            for sql_idx in range(len(sqls)):
                sql = sqls[sql_idx]
                cursor.execute(sql)
            self.con.commit()
            logging.info("Success in creating table.")
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in creating table.")
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
        finally:
            cursor.close()



################################### PART3 CLASS TEST ##################################
# initial parameters
database_name = "messageDB"
table_name_list = ["message_table", "word_table"]


Creater = createDatabaseTable()
Creater.create_database(database_name = database_name)
Creater.create_table(database_name = database_name, table_name_list = table_name_list)