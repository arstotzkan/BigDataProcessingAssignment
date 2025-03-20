import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

config = configparser.ConfigParser()
config.read('config.ini')

e, k = int(config["query_config"]["e"]), int(config["query_config"]["k"])

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("x", IntegerType(), True),
    StructField("y", IntegerType(), True)
])


r_df = spark.read.csv("data/RAILS.csv", sep="\t", header=False, schema=schema) 
s_df = spark.read.csv("data/AREALM.csv", sep="\t", header=False, schema=schema) 
