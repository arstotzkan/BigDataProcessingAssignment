import time
import math
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    ArrayType, StructType
)

# Args
r_set_path, s_set_path, output_path = sys.argv[1], sys.argv[2], sys.argv[3]
e, x_cells, y_cells = float(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])

start_time = time.time()
spark = SparkSession.builder.getOrCreate()

# Schemas
r_schema = StructType([
    StructField("id", StringType(), True),
    StructField("r_x", DoubleType(), True),
    StructField("r_y", DoubleType(), True)
])
s_schema = StructType([
    StructField("id", StringType(), True),
    StructField("s_x", DoubleType(), True),
    StructField("s_y", DoubleType(), True)
])

# Load R and S
r_df = spark.read.csv(r_set_path, sep="\t", header=False, schema=r_schema)\
    .withColumn("dataset", F.lit("r"))
s_df = spark.read.csv(s_set_path, sep="\t", header=False, schema=s_schema)\
    .withColumn("dataset", F.lit("s"))

min_x = min(r_df.select(F.min("r_x")).collect()[0][0], s_df.select(F.min("s_x")).collect()[0][0])
min_y = min(r_df.select(F.min("r_y")).collect()[0][0], s_df.select(F.min("s_y")).collect()[0][0])

max_x= max(r_df.select(F.max("r_x")).collect()[0][0], s_df.select(F.max("s_x")).collect()[0][0])
max_y = max(r_df.select(F.max("r_y")).collect()[0][0], s_df.select(F.max("s_y")).collect()[0][0])



# Define cell assignment for S
cell_size_x = (max_x - min_x) / x_cells
cell_size_y = (max_y - min_y) / y_cells

s_df = s_df.withColumn("cell_x", F.floor(F.col("s_x") / cell_size_x).cast(IntegerType())) \
           .withColumn("cell_y", F.floor(F.col("s_y") / cell_size_y).cast(IntegerType()))

# Group S points into grid cells
s_grouped = s_df.groupBy("cell_x", "cell_y").agg(
    F.collect_list(F.struct("id", "s_x", "s_y")).alias("s_points")
)

# Assign R to its own grid cell
r_df = r_df.withColumn("cell_x", F.floor(F.col("r_x") / cell_size_x).cast(IntegerType())) \
           .withColumn("cell_y", F.floor(F.col("r_y") / cell_size_y).cast(IntegerType()))

# UDF to generate 3x3 neighbor cells
def get_neighbor_cells(cx, cy):
    return [(cx + dx, cy + dy) for dx in [-1, 0, 1] for dy in [-1, 0, 1]]

neighbor_schema = ArrayType(StructType([
    StructField("cell_x", IntegerType(), False),
    StructField("cell_y", IntegerType(), False)
]))

get_neighbor_cells_udf = F.udf(get_neighbor_cells, neighbor_schema)

# Expand R to its neighbor cells
r_neighbors = r_df.withColumn("neighbors", get_neighbor_cells_udf(F.col("cell_x"), F.col("cell_y"))) \
                  .withColumn("neighbor", F.explode("neighbors")) \
                  .withColumn("n_cell_x", F.col("neighbor.cell_x")) \
                  .withColumn("n_cell_y", F.col("neighbor.cell_y")) \
                  .drop("neighbors", "neighbor")

# Join R with nearby S cells
joined = r_neighbors.join(
    s_grouped,
    (r_neighbors.n_cell_x == s_grouped.cell_x) & (r_neighbors.n_cell_y == s_grouped.cell_y),
    how="left"
)

result_schema = ArrayType(StructType([
    StructField("r_id", StringType(), True),
    StructField("s_id", StringType(), True)
]))


# Apply distance filter
filtered = joined.withColumn("s_point", F.explode("s_points")) \
    .filter(
        ((F.col("r_x") - F.col("s_point.s_x")) ** 2 +
         (F.col("r_y") - F.col("s_point.s_y")) ** 2) <= e * e
    ) \
    .select(
        F.col("id").alias("r_id"),
        F.col("s_point.id").alias("s_id")
    )

# Save output
result = filtered.count()
#TODO: write into file
print("Result:", result)
print("Done in", time.time() - start_time, "seconds.")
