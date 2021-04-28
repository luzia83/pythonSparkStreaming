import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions


def main(directory) -> None:
    """ Program that reads temperatures in streaming from a directory, computing the average temperature.

    It is assumed that an external entity is writing files in that directory, and every file contains a
    temperature value.

    :param directory: streaming directory
    """
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("StreamingAverageTemperature") \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines 
    lines = spark \
        .readStream \
        .format("text") \
        .load(directory)

    lines.printSchema()

    # Compute the average temperature
    values = lines.select(functions.avg(lines["value"]))

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingAverageTemperature <directory>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
