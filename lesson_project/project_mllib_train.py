# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegression



spark = SparkSession.builder.appName("mllib_prepare_and_train_app").getOrCreate()

data_path = "ml_data/water_potability.csv"
model_dir = "ml_data/models"

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
        .setRawPredictionCol("prediction")


# PREPARE AND TRAIN #
# ============================= #
def prepare_data(data, features, target):
    # features
    f_columns = ",".join(features).split(",")
    print(f_columns)
    # target
    f_target = ",".join(target).split(",")
    f_target = list(map(lambda c: F.col(c).alias("label"), f_target))
    print(f_target)
    # all columns
    all_columns = ",".join(features + target).split(",")
    all_columns = list(map(lambda c: F.col(c), all_columns))
    print(all_columns)
    # model data set
    model_data = data.select(all_columns)
    #fill null with medians
    model_data = model_data.na.fill({'ph':7.04, 'Sulfate':333, 'Trihalomethanes':66.6})
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    model_data = model_data.select('features', f_target[0])
    # standartization
    scaler = StandardScaler(inputCol='features', outputCol='features' + '_sc')
    model_data = scaler.fit(model_data).transform(model_data)
    model_data = model_data.select('features_sc', 'label')
    return model_data


def prepare_and_train(data, features, target):
    model_data = prepare_data(data, features, target)
    # train, test
    train, test = model_data.randomSplit([0.8, 0.2], seed=12345)
    # model
    rf = LogisticRegression(labelCol="label", featuresCol="features_sc", maxIter=10, regParam=0.01)
    # train model
    model = rf.fit(train)
    # check the model on the test data
    prediction = model.transform(test)
    evaluation_result = evaluator.evaluate(prediction)
    print("Evaluation result: {}".format(evaluation_result))
    return model


# MODEL #
print("=== MODEL ===")
features = ["ph,Hardness,Solids,Chloramines,Sulfate,Conductivity,Organic_carbon,Trihalomethanes,Turbidity"]

model = prepare_and_train(data, features, target)
model.write().overwrite().save(model_dir + "/model_lr")

