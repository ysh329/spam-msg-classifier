# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_logging.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-11-10 10:38:55
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import time

################################### PART2 CLASS && FUNCTION ###########################
class Logging(object):
    def __init__(self, log_data_dir):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = log_data_dir,
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("CLASS %s START." % Logging.__name__)



    def __del__(self):
        logging.info("CLASS %s END." % Logging.__name__)

        self.end = time.clock()
        logging.info("The class %s run time is : %.03f seconds" % (Logging.__name__, self.end - self.start))



    def error(self, error_str):
        logging.error(error_str)



    def info(self, info_str):
        logging.info(info_str)



################################### PART3 CLASS TEST ##################################
log_data_dir = './main.log'
LogGenerator = Logging(log_data_dir = log_data_dir)