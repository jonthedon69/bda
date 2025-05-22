from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read input text file as RDD
text_rdd = spark.read.text("input.txt").rdd

# Perform word count using RDD transformations
word_counts = (
    text_rdd.flatMap(lambda line: line[0].split())  # Split lines into words
    .map(lambda word: (word.lower(), 1))            # Map each word to (word, 1)
    .reduceByKey(lambda a, b: a + b)                # Reduce by key (word)
)

# Collect and print results
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Stop Spark session
spark.stop()

#to run spark-submit wordcountspark.py
