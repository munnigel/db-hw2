import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    concat_ws,
    split,
    explode,
    col,
    trim,
)

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 3").getOrCreate()

# Read the CSV file
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

# Extract reviews and dates using regexp_extract
df = df.withColumn(
    "reviews",
    regexp_extract(col("Reviews"), r"(\[.*?\])", 1),
).withColumn(
    "dates",
    regexp_extract(col("Reviews"), r"(\[.*?\])", 2),
)

# Clean up extracted arrays by removing brackets and quotes
df = df.withColumn("reviews", regexp_replace(col("reviews"), r"[\[\]']", ""))
df = df.withColumn("dates", regexp_replace(col("dates"), r"[\[\]']", ""))

# Split the extracted strings into arrays
df = df.withColumn("reviews", split(col("reviews"), ", ")).withColumn(
    "dates", split(col("dates"), ", ")
)

# Create separate rows for each review-date pair using explode
df = df.select("ID_TA", explode(arrays_zip("reviews", "dates")).alias("review_date"))

# Extract review and date columns from the exploded struct
df = df.select("ID_TA", col("review_date.0").alias("review"), col("review_date.1").alias("date"))

# Remove leading and trailing whitespace characters
df = df.withColumn("review", trim(col("review"))).withColumn("date", trim(col("date")))

# Show the DataFrame
df.show()

# Write the output as CSV into the HDFS path
df.write.csv("hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn), header=True)
