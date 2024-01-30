#this file is used for loading configurations from sbdl.conf and spark.conf file 

import configparser
from pyspark import SparkConf

def get_config(env):  #this function reads the sbdl.conf file, place all the configurations
                      #in a python directory and return it.
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf


def get_spark_conf(env): #this function will read spark.conf file
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    
    for (key,val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf

def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter]  == "" else conf[data_filter]


#the idea of get_config function is to read sbdl.conf only once and keep all the configurations in the memory so we can use it as and when needed. 
#the get_config function takes current env as input , it can be (local,QA,prod)
#the get_spark_conf function creates a sparkconf object & adds all the configurations in SparkConf.
#the get_data_filter will help us build a where clause at runtime.