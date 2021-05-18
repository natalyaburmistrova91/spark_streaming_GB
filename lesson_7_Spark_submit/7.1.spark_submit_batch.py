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

spark = SparkSession.builder.appName("spark-submit-batch-app").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

raw_files = spark \
    .read \
    .format("csv") \
    .schema(schema) \
    .options(path=source_path, header=True) \
    .load()


# fix timestamp
load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
print("START BATCH LOADING. TIME = " + load_time)


# File output #
# =========== #
def write_to_file(df):
    return df.write \
        .mode("append") \
        .parquet(output_path + "/p_date=" + str(load_time))


# write parquet files in partitions
modified_df = raw_files.withColumn("p_date", F.lit("load_time"))

# write modified df to a FS
write_to_file(modified_df)


print("FINISHED BATCH LOADING. TIME = " + load_time)

spark.stop()
