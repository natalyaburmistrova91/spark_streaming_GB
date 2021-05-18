
# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[4]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType


spark = SparkSession.builder.appName("spark_sinks_app").getOrCreate()

student_acc = "YOUR_ACCOUNT_NAME"

student_full_account = "student_1004_" + student_acc

in_memory_table_name = "memory_output_table"
output_path = "tmp/{}/orders_file_output".format(student_full_account)
checkpoint_location = "tmp/{}/orders_checkpoint".format(student_full_account)

# Read stream from Kafka #
# ====================== #
source_topic = "{}_lesson4_orders_json_topic".format(student_full_account)
target_row_topic = "{}_lesson4_orders_row_modified_topic".format(student_full_account)
target_json_topic = "{}_lesson4_orders_json_modified_topic".format(student_full_account)

kafka_brokers = "bigdataanalytics2-worker-shdpt-v31-1-1:6667"

# read stream
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", source_topic). \
    option("maxOffsetsPerTrigger", "50"). \
    option("startingOffsets", "earliest"). \
    load()

schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("order_status", StringType()) \
    .add("order_purchase_timestamp", StringType()) \
    .add("order_approved_at", StringType()) \
    .add("order_delivered_carrier_date", StringType()) \
    .add("order_delivered_customer_date", StringType()) \
    .add("order_estimated_delivery_date", StringType())

parsed_orders = raw_orders \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset")

# Console output
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .options(truncate=False) \
        .start()

stream = console_output(parsed_orders, 5)
stream.stop()


# Memory output #
# ============= #
def memory_output(df, freq):
    return df.writeStream.format("memory") \
        .queryName(in_memory_table_name) \
        .trigger(processingTime='%s seconds' % freq) \
        .outputMode("append") \
        .start()

stream = memory_output(parsed_orders, 5)

# show current in-memory output state
in_memory_df = spark.sql("select * from {}".format(in_memory_table_name))
in_memory_df.show()
#
delivered_orders_in_memory_df = spark.sql(
    """
    select *, current_timestamp() as ts_column 
    from {} 
    where order_status = "delivered" 
    """.format(in_memory_table_name))
delivered_orders_in_memory_df.show()

stream.stop()


# File output #
# =========== #
def file_output(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

stream = file_output(parsed_orders, 30)
stream.stop()


# Kafka output #
# ============ #
# as bytes value
kafka_row_formatted_output = parsed_orders.selectExpr("CAST(null AS STRING) as key", "CAST(struct(*) AS STRING) as value")

out = console_output(kafka_row_formatted_output, 5)
out.stop()

def kafka_output(df, freq, topic):
    return df \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("topic", topic) \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

stream = kafka_output(kafka_row_formatted_output, 5, target_row_topic)
stream.stop()

# as json value
kafka_json_formatted_output = parsed_orders.selectExpr("CAST(null AS STRING) as key", "CAST(to_json(struct(*)) AS STRING) as value")

out = console_output(kafka_json_formatted_output, 5)
out.stop()

stream = kafka_output(kafka_json_formatted_output, 5, target_json_topic)
stream.stop()


# ForeachBatch as custom output (Since Spark 2.4)
extended_orders = parsed_orders \
    .withColumn("random_digit_column", F.round(F.rand() * 100)) \
    .withColumn("ts_column", F.current_timestamp())


def foreach_batch_function(df, epoch_id):
    print("starting epoch " + str(epoch_id) )
    df.cache()
    df.filter(F.col("order_status") != "delivered"). \
        select("order_id", "order_status"). \
        show(truncate=False)
    df.filter(F.col("order_status") == "delivered"). \
        select("order_id", "order_status"). \
        show(truncate=False)
    df.unpersist()
    print("finishing epoch " + str(epoch_id))


def foreach_batch_sink(df, freq):
    return df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq) \
        .start()


stream = foreach_batch_sink(extended_orders, 20)
stream.stop()


# with persist
def foreach_batch_function(df, epoch_id):
    print("Batch id: {} is in progress".format(epoch_id))
    df.cache()
    delivered = df.filter(F.col("order_status") == "delivered")
    delivered.cache()
    delivered.show()
    delivered.write.mode("append").parquet(output_path + "/delivered")
    print("Deliveried count: " + str(delivered.count()))
    delivered.unpersist()
    not_delivered = df.filter(F.col("order_status") != "delivered")
    not_delivered.cache()
    not_delivered.show()
    not_delivered.write.mode("append").parquet(output_path + "/not_delivered")
    print("Not delivered count: " + str(not_delivered.count()))
    not_delivered.unpersist()
    print("I FINISHED THE BATCH")


stream = foreach_batch_sink(extended_orders, 30)
stream.stop()
