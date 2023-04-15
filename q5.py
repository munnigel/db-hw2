import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, array, array_sort, countDistinct, collect_list

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
)

df = df.drop("crew")

df = df.withColumn("cast_list", split(col("cast"), ", "))

# Create pairs of actors by collecting all actor pairs that co-occur in the same movie
df_pairs = (
    df.select("movie_id", "title", explode("cast_list").alias("actor1"))
    .join(df.select("movie_id", "cast_list"), "movie_id")
    .withColumn("actor2", explode("cast_list"))
    .filter(col("actor1") != col("actor2"))
)

# Group by movie_id, title, actor1, and actor2, then count distinct movies for each pair
df_pairs = (
    df_pairs.groupBy("movie_id", "title", "actor1", "actor2")
    .agg(countDistinct("movie_id").alias("movies_count"))
)

# Filter only pairs that co-cast in at least 2 movies
df_pairs = df_pairs.filter(col("movies_count") >= 2)

# Write the output to HDFS in Parquet format
df_pairs.write.option("header", True).mode("overwrite").parquet(
    "hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn)
)
