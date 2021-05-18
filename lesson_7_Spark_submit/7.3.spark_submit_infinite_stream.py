from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime


# current user name
student_acc = "YOUR_ACCOUNT_NAME"
student_full_account = "student_1004_" + student_acc

# source path
source_path = '{}/process_csv_as_stream'.format(student_full_account)
output_path = "/tmp/{}/file_output".format(student_full_account)
checkpoint_location = "/tmp/{}/checkpoint".format(student_full_account)

spark = SparkSession.builder.appName("spark-submit-infinite-stream-app").getOrCreate()


schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

# read all csv in stream mode
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path=source_path, header=True) \
    .load()


#
def file_sink(df, freq):
    date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq) \
        .option("checkpointLocation", checkpoint_location + "/" + date) \
        .start()


# for every batch use separate directory
def foreach_batch_function(df, epoch_id):
    load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    print("START BATCH LOADING. TIME = " + load_time)
    df.withColumn("p_date", F.lit("load_time")) \
        .write \
        .mode("append") \
        .parquet(output_path + "/p_date=" + str(load_time))
    print("FINISHED BATCH LOADING. TIME = " + load_time)


stream = file_sink(raw_files, 10)

# run endless cycle
stream.awaitTermination()

# unreachable
spark.stop()
