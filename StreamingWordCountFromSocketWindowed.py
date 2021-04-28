from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, window
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StreamingWordCountFromSocket") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .option('includeTimestamp', 'true') \
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word"), lines.timestamp)


words.printSchema()

# Generate running word count, indicating window parameters (in seconds)
windowSize = '{} seconds'.format(20)
slideSize = '{} seconds'.format(15)
windowedCounts = words.groupBy(
        window(words.timestamp, windowSize, slideSize),
        words.word
    ).count()\
    .orderBy('window')

windowedCounts.printSchema()

# Start running the query that prints the running counts to the console
query = windowedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option('truncate', 'false') \
    .start()

query.awaitTermination()

