
# Run Pyspark as:
# pyspark --master local

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

# create spark session
spark = SparkSession\
    .builder\
    .appName("file_stream_app")\
    .getOrCreate()

# current user name
current_user_name = 'YOUR_USER_NAME'

# source path
source_path = 'user/{}/process_csv_as_stream'.format(current_user_name)


def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .options(truncate=False) \
        .start()


schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

# schema v1
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path=source_path, header=True) \
    .load()

# schema v2
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema("product_category_name STRING, product_category_name_english STRING") \
    .options(path=source_path, header=True) \
    .load()


out = console_output(raw_files, 5)
out.stop()

raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path=source_path,
             header=True,
             maxFilesPerTrigger=5) \
    .load()

out = console_output(raw_files, 5)
out.stop()


# data processing
extra_files = raw_files \
    .withColumn("spanish_length", F.length(F.col("product_category_name"))) \
    .withColumn("english_length", F.length(F.col("product_category_name_english"))) \
    .filter(F.col("spanish_length") == F.col("english_length"))

out = console_output(extra_files, 5)
out.stop()
