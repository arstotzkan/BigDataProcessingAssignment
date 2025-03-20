import configparser
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('config.ini')

e, k = int(config["query_config"]["e"]), int(config["query_config"]["k"])

spark = SparkSession.builder.getOrCreate()

r_df = spark.read.csv("data/RAILS.csv", sep="\t") 
s_df = spark.read.csv("data/AREALM.csv", sep="\t") 
