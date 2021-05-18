#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

import json

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


spark = SparkSession.builder.appName("mllib_predict_app").getOrCreate()

student_acc = "YOUR_ACCOUNT_NAME"
student_full_account = "student_1004_" + student_acc
checkpoint_location = "tmp/{}/ml_checkpoint".format(student_full_account)


# upload the best model
data_path = "/ml_data/*"
model_dir = "/ml_data/models"
model = LinearRegressionModel.load(model_dir + "/model_2")

# read stream
schema = StructType.fromJson(json.loads('{"fields":[{"metadata":{},"name":"date","nullable":true,"type":"timestamp"},{"metadata":{},"name":"price","nullable":true,"type":"double"},{"metadata":{},"name":"bedrooms","nullable":true,"type":"double"},{"metadata":{},"name":"bathrooms","nullable":true,"type":"double"},{"metadata":{},"name":"sqft_living","nullable":true,"type":"integer"},{"metadata":{},"name":"sqft_lot","nullable":true,"type":"integer"},{"metadata":{},"name":"floors","nullable":true,"type":"double"},{"metadata":{},"name":"waterfront","nullable":true,"type":"integer"},{"metadata":{},"name":"view","nullable":true,"type":"integer"},{"metadata":{},"name":"condition","nullable":true,"type":"integer"},{"metadata":{},"name":"sqft_above","nullable":true,"type":"integer"},{"metadata":{},"name":"sqft_basement","nullable":true,"type":"integer"},{"metadata":{},"name":"yr_built","nullable":true,"type":"integer"},{"metadata":{},"name":"yr_renovated","nullable":true,"type":"integer"},{"metadata":{},"name":"street","nullable":true,"type":"string"},{"metadata":{},"name":"city","nullable":true,"type":"string"},{"metadata":{},"name":"statezip","nullable":true,"type":"string"},{"metadata":{},"name":"country","nullable":true,"type":"string"}],"type":"struct"}'))

features = ["bedrooms,bathrooms,sqft_living,sqft_lot,floors,waterfront,view,condition,sqft_above,sqft_basement,yr_built,yr_renovated,city,country"]

data = spark\
    .readStream\
    .format("csv")\
    .schema(schema)\
    .options(header=True, maxFilesPerTrigge=1) \
    .load(data_path)


def prepare_data(df, features):
    # features
    f_columns = ",".join(features).split(",")
    # model data set
    model_data = df.select(f_columns)
    # prepare categorical columns
    text_columns = ['city', 'country']
    output_text_columns = [c + "_index" for c in text_columns]
    for c in text_columns:
        string_indexer = StringIndexer(inputCol=c, outputCol=c + '_index').setHandleInvalid("keep")
        model_data = string_indexer.fit(model_data).transform(model_data)
    # update f_columns with indexed text columns
    f_columns = list(filter(lambda c: c not in text_columns, f_columns))
    f_columns += output_text_columns
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    return model_data.select('features')


def process_batch(df, epoch):
    model_data = prepare_data(df, features)
    prediction = model.transform(model_data)
    prediction.show()


def foreach_batch_output(df):
    from datetime import datetime as dt
    date = dt.now().strftime("%Y%m%d%H%M%S")
    return df\
        .writeStream \
        .trigger(processingTime='%s seconds' % 10) \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_location + "/" + date)\
        .start()

stream = foreach_batch_output(data)

stream.awaitTermination()
