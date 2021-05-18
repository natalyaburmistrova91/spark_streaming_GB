# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

#from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("mllib_prepare_and_train_app").getOrCreate()

data_path = "/ml_data/water_potability.csv"
model_dir = "/ml_data/models"

# read data from storage
data = spark\
    .read\
    .format("csv")\
    .options(inferSchema=True, header=True) \
    .load(data_path)

data.show(3)
print(data.schema.json())

# target
target = ["Potability"]

# model evaluator
evaluator = BinaryClassificationEvaluator() \
        .setMetricName("areaUnderROC") \
        .setLabelCol("label") \
        .setPredictionCol("prediction")


# PREPARE AND TRAIN - VERSION 1 #
# ============================= #
def prepare_data_1(data, features, target):
    # features
    f_columns = ",".join(features).split(",")
    # target
    f_target = ",".join(target).split(",")
    f_target = list(map(lambda c: F.col(c).alias("label"), f_target))
    # all columns
    all_columns = ",".join(features + target).split(",")
    all_columns = list(map(lambda c: F.col(c), all_columns))
    # model data set
    model_data = data.select(all_columns)
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    model_data = model_data.select('features', f_target[0])
    return model_data


def prepare_and_train_1(data, features, target):
    model_data = prepare_data_1(data, features, target)
    # train, test
    train, test = model_data.randomSplit([0.8, 0.2], seed=12345)
    # model
    lr = LinearRegression(featuresCol='features', labelCol='label', maxIter=10, regParam=0.01)
    # train model
    model = lr.fit(train)
    # check the model on the test data
    prediction = model.transform(test)
    prediction.show(5)
    evaluation_result = evaluator.evaluate(prediction)
    print("Evaluation result: {}".format(evaluation_result))
    return model


# MODEL 1 #
print("=== MODEL 1 ===")
features = ["bedrooms,bathrooms,sqft_living,sqft_lot,floors,waterfront,view,condition,sqft_above,sqft_basement,yr_built,yr_renovated"]

model_1 = prepare_and_train_1(data, features, target)
model_1.write().overwrite().save(model_dir + "/model_1")

