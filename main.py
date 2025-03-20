import configparser
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('config.ini')

e, k = int(config["query_config"]["e"]), int(config["query_config"]["k"])

spark = SparkSession.builder.getOrCreate()

