# -*- coding: utf-8 -*-
# !/usr/bin/python
import ConfigParser
#生成config对象
conf = ConfigParser.ConfigParser()
#用config对象读取配置文件
conf.read("../config.ini")

'''
#以列表形式返回所有的section
sections = conf.sections()
print 'sections:', sections         #sections: ['sec_b', 'sec_a']

#得到指定section的所有option
options = conf.options("basic")
print 'options:', options           #options: ['a_key1', 'a_key2']

#得到指定section的所有键值对
kvs = conf.items(sections[0])
print 'sec_a:', kvs                 #sec_a: [('a_key1', '20'), ('a_key2', '10')]
'''


#指定section，option读取值
appName = conf.get("basic", "appName")
database_name = conf.get("database", "database_name")
database_password = conf.get("database", "database_password")
message_table_name = conf.get("database", "message_table_name")
word_table_name = conf.get("database", "word_table_name")
print "appName: {appName}".format(appName = appName)
print "database_name: {database_name}".format(database_name = database_name)
print "database_password: {database_password}".format(database_password = database_password)
print "message_table_name: {message_table_name}".format(message_table_name = message_table_name)
print "word_table_name: {word_table_name}".format(word_table_name = word_table_name)
#int_val = conf.getint("database", "password")

#print "value for sec_a's a_key1:", str_val   #value for sec_a's a_key1: 20
#print "value for sec_a's a_key2:", int_val   #value for sec_a's a_key2: 10


train_data_dir = conf.get("data", "train_data_dir")
test_data_dir = conf.get("data", "test_data_dir")
stopword_data_dir = conf.get("data", "stopword_data_dir")

print "train_data_dir: {train_data_dir}".format(train_data_dir = train_data_dir)
print "test_data_dir: {test_data_dir}".format(test_data_dir = test_data_dir)
print "stopword_data_dir: {stopword_data_dir}".format(stopword_data_dir = stopword_data_dir)