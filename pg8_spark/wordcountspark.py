#in terminal first do pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read input text file
text_rdd = spark.read.text("input.txt").rdd

# Perform word count
word_counts = (
    text_rdd.flatMap(lambda line: line[0].split())
            .map(lambda word: (word.lower(), 1))
            .reduceByKey(lambda a, b: a + b)
)

# Print the result
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Stop the session
spark.stop()
#to run   spark-submit wordcountspark.py
