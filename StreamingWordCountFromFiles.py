import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def main(file) -> None:
    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("StreamingWordCountFromFiles") \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("text") \
        .option("header", "true") \
        .load(file)

    lines.printSchema()

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    words.printSchema()

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingWordCountFromFiles <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
