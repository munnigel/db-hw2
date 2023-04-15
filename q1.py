import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 1").getOrCreate()

# Read the CSV file
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

# Filter rows using Spark SQL expressions
df_filtered = df.filter(
    expr("Reviews != '[]' AND Reviews IS NOT NULL") &
    expr("Rating >= 1.0 AND Rating IS NOT NULL")
)

# Show the filtered DataFrame
df_filtered.show()

# Write the output as CSV into HDFS path
df_filtered.write.csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)

