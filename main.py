import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import sqrt, pow

config = configparser.ConfigParser()
config.read('config.ini')

e, k = int(config["query_config"]["e"]), int(config["query_config"]["k"])

spark = SparkSession.builder.getOrCreate()

r_schema = StructType([
    StructField("r_id", IntegerType(), True),
    StructField("r_x", IntegerType(), True),
    StructField("r_y", IntegerType(), True)
])

s_schema = StructType([
    StructField("s_id", IntegerType(), True),
    StructField("s_x", IntegerType(), True),
    StructField("s_y", IntegerType(), True)
])

r_df = spark.read.csv("data/RAILS.csv", sep="\t", header=False, schema=r_schema) 
s_df = spark.read.csv("data/AREALM.csv", sep="\t", header=False, schema=s_schema) 

df_cross = r_df.join(s_df, (sqrt(pow(r_df.r_x - s_df.s_x, 2) + pow(r_df.r_y - s_df.s_y, 2)) < e))

with open("output/resultsA.txt", "w") as f1:
    f1.write(str(df_cross.count()))
