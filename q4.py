import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace, explode, count, col, trim

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 4").getOrCreate()

# Read the CSV file
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

# Clean and split the "Cuisine Style" column
df = df.withColumn(
    "Cuisine Style",
    split(regexp_replace(col("Cuisine Style"), r"[\[\]']", ""), ", "),
)

# Explode the "Cuisine Style" column and clean up the exploded values
df = df.select(
    "City",
    trim(explode(col("Cuisine Style"))).alias("Cuisine"),
)

# Group by "City" and "Cuisine" and count the number of rows in each group
result = (
    df.groupBy("City", "Cuisine")
    .count()
    .sort(col("City").asc(), col("count").desc())
)

# Show the DataFrame and write the output as CSV into the specified HDFS path
result.show()
result.write.csv("hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn), header=True)


