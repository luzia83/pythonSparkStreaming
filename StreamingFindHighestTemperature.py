import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import current_timestamp, window
from pyspark.sql.types import StructField, DoubleType, StringType, TimestampType, StructType

def main(directory) -> None:
    """ Program that reads temperatures in streaming from a directory, finding those that are higher than a given
    threshold.

    It is assumed that an external entity is writing files in that directory, and every file contains a
    temperature value.

    :param directory: streaming directory
    """
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("StreamingFindHighTemperature") \
        .getOrCreate()

    fields = [StructField("sensor", StringType(), True),
              StructField("value", DoubleType(), True),
              StructField("timestamp", TimestampType(), True)]

    # Create DataFrame
    lines = spark \
        .readStream \
        .format("csv") \
        .schema(StructType(fields)) \
        .load(directory)

    lines.printSchema()

    windows_size = 20
    slide_size = 20

    window_duration = '{} seconds'.format(windows_size)
    slide_duration = '{} seconds'.format(slide_size)

    # Compute the maximum temperature

    values = lines.groupBy(
        window(lines.timestamp, window_duration, slide_duration), lines.sensor) \
        .agg(functions.max("value").alias("Maximum")) \
        .orderBy("sensor", "window")

    values.printSchema()

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option('truncate', 'false') \
        .option("numRows", "100")\
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingFindHighestTemperature <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
