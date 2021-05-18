
# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 --driver-memory 512m --master local[4] --conf spark.sql.shuffle.partitions=20

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

spark = SparkSession.builder.appName("stream_aggregation_app").getOrCreate()

kafka_brokers = "bigdataanalytics2-worker-shdpt-v31-1-1:6667"

student_acc = "YOUR_ACCOUNT_NAME"
student_full_account = "student_1004_" + student_acc
checkpoint_location = "tmp/{}/orders_checkpoint".format(student_full_account)

# read orders from kafka as a stream
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "earliest"). \
    load()

# kafka value schema
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("order_status", StringType()) \
    .add("order_purchase_timestamp", StringType()) \
    .add("order_approved_at", StringType()) \
    .add("order_delivered_carrier_date", StringType()) \
    .add("order_delivered_customer_date", StringType()) \
    .add("order_estimated_delivery_date", StringType())

value_orders = raw_orders \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

# adding timestamp column to orders
orders_with_ts = value_orders.select("value.order_id", "value.order_status") \
                    .withColumn("order_receive_time", F.current_timestamp())


orders_with_ts.printSchema()

# Console output
def console_output(df, freq):
    date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("checkpointLocation", checkpoint_location + "/" + date) \
        .options(truncate=False) \
        .start()


stream = console_output(orders_with_ts, 2)
stream.stop()


# Watermark #
# ========= #
# One common collected batch
waterwarked_orders = orders_with_ts.withWatermark("order_receive_time", "1 hour")
waterwarked_orders.printSchema()
stream = console_output(waterwarked_orders, 20)
stream.stop()

# Records de-duplication
deduplicated_orders = waterwarked_orders.drop_duplicates(["order_status", "order_receive_time"])

stream = console_output(deduplicated_orders, 20)
stream.stop()


# Window function - creates a bucket for an interval
windowed_orders = orders_with_ts.withColumn("window_time", F.window(F.col("order_receive_time"), "2 minute"))
windowed_orders.printSchema()

stream = console_output(windowed_orders, 20)
stream.stop()

# Setting a Watermark for a bucket
waterwarked_windowed_orders = windowed_orders.withWatermark("window_time", "1 minute")
stream = console_output(waterwarked_windowed_orders, 10)
stream.stop()

# De-duplication
deduplicated_windowed_orders = waterwarked_windowed_orders.drop_duplicates(["order_status", "window_time"])

stream = console_output(deduplicated_windowed_orders, 10)
stream.stop()


# Sliding window #
sliding_orders = orders_with_ts.withColumn("sliding_time", F.window(F.col("order_receive_time"), "1 minute", "30 seconds"))
sliding_orders.printSchema()

stream = console_output(sliding_orders, 20)
stream.stop()

deduplicated_sliding_orders = sliding_orders.drop_duplicates(["order_status", "order_receive_time"])
stream = console_output(deduplicated_sliding_orders, 20)
stream.stop()


# Set watermark on a sliding window
waterwarked_sliding_orders = sliding_orders.withWatermark("sliding_time", "2 minutes")

# drop duplicates
deduplicated_sliding_orders = waterwarked_sliding_orders.drop_duplicates(["order_status", "sliding_time"])

stream = console_output(deduplicated_sliding_orders , 20)
stream.stop()



# Output modes
def console_output(df, freq, out_mode):
    date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return df.writeStream.format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .options(truncate=False) \
        .option("checkpointLocation", checkpoint_location + "/" + date) \
        .outputMode(out_mode) \
        .start()


windowed_orders = orders_with_ts.withColumn("window_time", F.window(F.col("order_receive_time"), "1 minute"))
waterwarked_windowed_orders = windowed_orders.withWatermark("window_time", "1 minute")

count_orders = waterwarked_windowed_orders.groupBy("window_time").count()

# update mode - all records with updated
stream = console_output(count_orders, 20, "update")
stream.stop()

# complete mode - all records
stream = console_output(count_orders, 20, "complete")
stream.stop()

# append mode - shows nothing because windows overlapping
stream = console_output(count_orders, 20, "append")
stream.stop()

# checking for count in the sliding windows
sliding_orders = waterwarked_sliding_orders.groupBy("sliding_time").count()
stream = console_output(sliding_orders , 20, "update")
stream.stop()


# Joins #
# ===== #
# stream-hdfs join
static_df = spark.read.load("tmp/orders_file_output/")
static_df.show()

static_joined = waterwarked_orders.join(static_df, "order_id", "inner")
static_joined.isStreaming
static_joined.printSchema()

selected_static_joined = static_joined.select("order_id", static_df.order_status, "order_receive_time")

stream = console_output(selected_static_joined, 20, "update")
stream.stop()

# stream-stream join
raw_orders_items = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "15"). \
    load()

extended_orders_items = raw_orders_items \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value")) \
    .select("value.*") \
    .withColumn("order_items_receive_time", F.current_timestamp()) \
    .withColumn("window_time", F.window(F.col("order_items_receive_time"),"10 minute"))


extended_orders_items.printSchema()
stream = console_output(extended_orders_items, 20, "append")
stream.stop()

windowed_orders = orders_with_ts.withColumn("window_time", F.window(F.col("order_receive_time"), "10 minute"))
waterwarked_windowed_orders = windowed_orders.withWatermark("window_time", "10 minute")

streams_joined = waterwarked_windowed_orders \
    .join(extended_orders_items, ["order_id", "window_time"] , "inner") \
    .select("order_id", "window_time")

# doesn't work for inner
stream = console_output(streams_joined, 20, "update")
stream = console_output(streams_joined, 20, "append")
stream.stop()

streams_joined = waterwarked_windowed_orders \
    .join(extended_orders_items, ["order_id", "window_time"] , "left") \
    .select("order_id", "window_time")

stream = console_output(streams_joined, 20, "update")
stream.stop()

