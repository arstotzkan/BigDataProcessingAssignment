import configparser
import math
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

print(sys.argv, 345678)
# TODO: argv[5,6] #grid limits

args = sys.argv

config = configparser.ConfigParser()
config.read('config.ini')

e = float(args[4])
k = float(args[7])

spark = SparkSession.builder.getOrCreate()

r_schema = StructType([
    StructField("r_id", StringType(), True),
    StructField("r_x", FloatType(), True),
    StructField("r_y", FloatType(), True)
])

s_schema = StructType([
    StructField("s_id", StringType(), True),
    StructField("s_x", FloatType(), True),
    StructField("s_y", FloatType(), True)
])

print("\n", args[1], args[2])
# 4. Read data
r_df = spark.read.csv(args[1], sep="\t", header=False, schema=r_schema)
s_df = spark.read.csv(args[2], sep="\t", header=False, schema=s_schema)

print("\n", r_df.count())
print("\n", s_df.count())

s_df.show(truncate=False)

# 5. Compute bounds of s_df
bounds_row = s_df.agg(
    F.min("s_x").alias("min_x"),
    F.max("s_x").alias("max_x"),
    F.min("s_y").alias("min_y"),
    F.max("s_y").alias("max_y")
).first()

print(123, bounds_row)
min_x, min_y, max_x, max_y = bounds_row["min_x"], bounds_row["min_y"], bounds_row["max_x"], bounds_row["max_y"]

overlap = 10  # spatial units to overlap
height = (max_y - min_y) / 3

def find_zone(y):
    if y < min_y + height + overlap:
        return 0
    elif y < min_y + 2 * height + overlap:
        return 1
    else:
        return 2
    
def is_in_region_1(y):
    return y < min_y + height + overlap

def is_in_region_2(y):
    return (y > min_y + height - overlap) and (y < min_y + 2 * height + overlap)

def is_in_region_3(y):
    return (y > min_y + 2 * height - overlap) and (y < min_y + 2 * height + overlap)


r_df = r_df.withColumn("r_region_1", is_in_region_1(F.col("r_y")))
r_df = r_df.withColumn("r_region_2", is_in_region_2(F.col("r_y")))
r_df = r_df.withColumn("r_region_3", is_in_region_3(F.col("r_y")))

s_df = s_df.withColumn("s_region_1", is_in_region_1(F.col("s_y")))
s_df = s_df.withColumn("s_region_2", is_in_region_2(F.col("s_y")))
s_df = s_df.withColumn("s_region_3", is_in_region_3(F.col("s_y")))

def process_partition(part_r, part_s):
    r_rows = list(part_r)
    s_rows = list(part_s)

    results = []
    for row_r in r_rows:
        for row_s in s_rows:
            dist = math.sqrt((row_r.r_x - row_s.s_x) ** 2 + (row_r.r_y - row_s.s_y) ** 2)
            if dist < e:
                results.append((row_r.r_id, row_s.s_id, dist))
    return iter(results)

r_rdd, s_rdd = r_df.rdd, s_df.rdd

neighbors_rdd = r_rdd.zipPartitions(s_rdd, preservesPartitioning=False)(process_partition)

neighbors_df = neighbors_rdd.toDF(["r_id", "s_id", "distance"])

neighbors_df.show()

df_string = neighbors_df.rdd.map(lambda row: ",".join([str(x) for x in row]))

print(88888888888, args[3])
df_string.saveAsTextFile(args[3])
