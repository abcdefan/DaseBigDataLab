from pyspark import SparkContext, SparkConf
import time
import psutil
import re
import sys
from pyspark.sql import SparkSession

# Reset SparkContext
SparkContext._gateway = None
SparkContext._jvm = None
SparkContext._jsc = None
SparkContext._active_spark_context = None

# Create new SparkSession
spark = SparkSession.builder \
    .appName("WordCount_1") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

sc = spark.sparkContext

process = psutil.Process()

start_time = time.time()

lines = sc.textFile("hdfs:///user/dase-dis/Odyssey.txt")

def count_words(a, b):
    return a + b

def word_count_func(line):
    cleaned_line = re.sub(r'[^a-zA-Z0-9\s]', '', line)
    words = cleaned_line.lower().split()
    return [(word, 1) for word in words]

word_counts = (
    lines.flatMap(word_count_func)
         .reduceByKey(count_words)
)

word_counts = word_counts.sortBy(lambda x: -x[1])

word_counts.saveAsTextFile("hdfs:///user/dase-dis/spark_output3")

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")