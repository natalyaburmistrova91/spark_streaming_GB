
# Run Pyspark as:
# pyspark --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder.appName("kafka_source_app").getOrCreate()

kafka_brokers = "bigdataanalytics2-worker-shdpt-v31-1-2:6667"


# Console output as a function
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .options(truncate=True) \
        .start()


# DataFrame in the batch mode #
# =========================== #
raw_orders = spark\
    .read\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_brokers)\
    .option("subscribe", "orders_json")\
    .option("startingOffsets", "earliest")\
    .load()

# Check on records structure
raw_orders.show()


# Read from the beginning to the specific offset
raw_orders = spark\
    .read\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_brokers)\
    .option("subscribe", "orders_json")\
    .option("startingOffsets", "earliest")\
    .option("endingOffsets", """{"orders_json":{"0": 20}}""")\
    .load()

raw_orders.show(40)


# Stream mode #
# =========== #
# Read all records from the beginning
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    load()

out = console_output(raw_orders, 5)
out.stop()

# poll batch size
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


# latest offset position
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "latest"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


# start from specific offset of a partition
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", """{"orders_json":{"0":10}}"""). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


# Change 'value' column type to STRING
value_orders = raw_orders \
    .select(F.col("value").cast("String"), "offset")

value_orders.printSchema()

out = console_output(value_orders, 30)
out.stop()


# schema of the json struct
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("order_status", StringType()) \
    .add("order_purchase_timestamp", StringType()) \
    .add("order_approved_at", StringType()) \
    .add("order_delivered_carrier_date", StringType()) \
    .add("order_delivered_customer_date", StringType()) \
    .add("order_estimated_delivery_date", StringType())

# extract json from string column 'value' and rename it as 'value'
from_json_orders = value_orders \
    .select(F.from_json(F.col("value"), schema).alias("order"), "offset")

from_json_orders.printSchema

out = console_output(from_json_orders, 30)
out.stop()


# Flat the value structure (alike SELECT t.* FROM table t)
parsed_orders = from_json_orders.select("order.*", "offset")

parsed_orders.printSchema()

out = console_output(parsed_orders, 30)
out.stop()


# use checkpoint for SINK
def console_output_checkpointed(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate", False) \
        .option("checkpointLocation", "orders_console_checkpoint") \
        .start()

out = console_output_checkpointed(parsed_orders, 5)
out.stop()
