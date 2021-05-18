#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]
# Firstly create keyspace in casandra and table in it
import json

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql.types import StructType
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("mllib_predict_app").getOrCreate()

student_acc = "393_burmistrova"
student_full_account = "student_1004_" + student_acc
checkpoint_location = "tmp/{}/ml_checkpoint".format(student_full_account)

keyspace = "student_393"

# upload the best model
data_path = "ml_data/*"
model_dir = "ml_data/models"
model = LogisticRegressionModel.load(model_dir + "/model")

# read stream
schema = StructType.fromJson(json.loads('{"fields":[{"metadata":{},"name":"ph","nullable":true,"type":"double"},{"metadata":{},"name":"Hardness","nullable":true,"type":"double"},{"metadata":{},"name":"Solids","nullable":true,"type":"double"},{"metadata":{},"name":"Chloramines","nullable":true,"type":"double"},{"metadata":{},"name":"Sulfate","nullable":true,"type":"double"},{"metadata":{},"name":"Conductivity","nullable":true,"type":"double"},{"metadata":{},"name":"Organic_carbon","nullable":true,"type":"double"},{"metadata":{},"name":"Trihalomethanes","nullable":true,"type":"double"},{"metadata":{},"name":"Turbidity","nullable":true,"type":"double"},{"metadata":{},"name":"Potability","nullable":true,"type":"integer"}],"type":"struct"}'))

features = ["ph,Hardness,Solids,Chloramines,Sulfate,Conductivity,Organic_carbon,Trihalomethanes,Turbidity"]

data = spark\
    .readStream\
    .format("csv")\
    .schema(schema)\
    .options(header=True, maxFilesPerTrigge=1) \
    .load(data_path)


def prepare_data(data, features):
    # features
    f_columns = ",".join(features).split(",")
    print(f_columns)
    # model data set
    model_data = data.select(f_columns)
    #fill null with medians
    model_data = model_data.na.fill({'ph':7.04, 'Sulfate':333, 'Trihalomethanes':66.6})
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    model_data = model_data.select('features')
    # standartization
    scaler = StandardScaler(inputCol='features', outputCol='features' + '_sc')
    model_data = scaler.fit(model_data).transform(model_data)
    return model_data.select('features_sc')


def process_batch(df, epoch):
    model_data = prepare_data(df, features)
    prediction = model.transform(model_data)
    prediction = prediction.select(col('rawPrediction').alias('rawprediction'), 'features_sc', 'probability', 'prediction')
    prediction.write\
        .format("org.apache.spark.sql.cassandra") \
        .mode("append")\
        .options(keyspace=keyspace, table="predictions") \
        .save()


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
