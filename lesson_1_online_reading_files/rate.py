from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# go to spark-2.4.7 folder and run ./bin/pyspark --master local

# Source (from files, systems, sockets) --> Sink (to files, systems, sockets)
# Source (from files, systems, sockets) --> (Spark processing) --> Sink (to files, systems, sockets)


# Spark session
spark = SparkSession \
    .builder \
    .appName("RateSourceApp") \
    .getOrCreate()


# Rate based DataFrame
rate_df = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()


# Output DataFrame's schema
rate_df.printSchema()


# Notes: Couple of differences between SQL DataFrame and Structured Streaming DataFrame
# 1. Structured Streaming DataFrame - isStreaming flag = True
rate_df.isStreaming

# 2. Structured Streaming DataFrame - show() function - doesn't work
# rate_df.show()


# Start stream with default triggering interval
rate_stream = rate_df \
    .writeStream \
    .format("console") \
    .start()

rate_stream.stop()

# another way to start streaming
stream = rate_df \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .format("console") \
    .option("truncate", False) \
    .start()


# Stream metrics and states
stream.explain()
stream.isActive
stream.lastProgress
stream.status


# Functional way
## Rate source
def rate_source(rps=1):
    return spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", rps) \
        .load()

## Stream to console
def console_output(df, freq):
    return df \
        .writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate", False) \
        .start()


stream_source = rate_source(5)
stream_sink = console_output(stream_source, 5)
stream_sink.awaitTermination()

# Stop stream
stream.stop()


# Processing examples
## Filter stream records
filtered_rate = rate_df \
    .filter(F.col("value") % F.lit("2") == 0)

stream = console_output(filtered_rate, 5)


## Addition of a new column
extra_rate = filtered_rate \
    .withColumn("is_jubilee", F.when(
        (F.col("value") % F.lit(10) == 0), F.lit("true")
    ).otherwise(F.lit("false")))

stream = console_output(extra_rate, 5)


# Использовать в случае когда нет ссылки на стрим
# То есть создание стрима было описано как - console_output(extra_rate, 5), а не stream = console_output(extra_rate, 5)
# Иначе не получится остановить стрим
def stop_all_streams():
    for active_stream in spark.streams.active:
        print("Stopping %s by killAll" % active_stream)
        active_stream.stop()
