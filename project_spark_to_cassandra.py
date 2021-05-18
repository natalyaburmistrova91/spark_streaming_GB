#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[2] --conf spark.sql.shuffle.partitions=20 --conf spark.cassandra.connection.host=localhost

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

spark = SparkSession.builder.appName("cassandra_spark_app").getOrCreate()

student_acc = "393_burmistrova"
student_full_account = "student_1004_" + student_acc
checkpoint_location = "tmp/{}/orders_checkpoint".format(student_full_account)

keyspace = "393_burmistrova_keyspace"


# write joined df to Cassandra as Stream by Batch
def foreach_batch(df, epoch):
    df.write\
        .format("org.apache.spark.sql.cassandra") \
        .mode("append")\
        .options(keyspace=keyspace, table="393_burmistrova") \
        .save()

def cassandra_output_batch(df, freq):
    from datetime import datetime as dt
    date = dt.now().strftime("%Y%m%d%H%M%S")
    return df\
        .writeStream\
        .trigger(processingTime='%s seconds' % 10) \
        .foreachBatch(foreach_batch)\
        .option("checkpointLocation", checkpoint_location + "/" + date)\
        .start()

stream = cassandra_output_batch(orders_by_names, 10)
stream.stop()


# Write to Cassandra #
'''
OPTION              DESCRIPTION                                             VALUE TYPE      DEFAULT
-------------------------------------------------------------------------------------------------------
table	            The Cassandra table to connect to	                    String	        N/A
keyspace	        The keyspace where table is looked for	                String	        N/A
cluster	            The group of the Cluster Level Settings to inherit	    String	        "default"
pushdown	        Enables pushing down predicates to C* when applicable	(true,false)	true
confirm.truncate	Confirm to truncate table when use Save.overwrite mode	(true,false)	false
-------------------------------------------------------------------------------------------------------
'''
# ================== #
names_df = spark.sql("""select '20b5aae6a3e31111009f9a7ecc31a232' as cid, 'Ann Peterson' as full_name""")
names_df.show()

# append
names_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace=keyspace, table="customer_names") \
    .mode("append") \
    .save()

# overwrite
names_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace=keyspace, table="customer_names") \
    .mode("overwrite") \
    .save()


# read all names
all_names_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace=keyspace, table="customer_names") \
    .load()

all_names_df.show()

# filter by cid
c_name_df = all_names_df.filter(F.col("cid") == "20b5aae6a3e31111009f9a7ecc31a232")
c_name_df.show()
c_name_df.count()

# filter by non-partition key
jane_df = all_names_df.filter(F.col("full_name") == "Ann Peterson")
jane_df.show()  # only first 20
jane_df.count()  # all records


# PushedFilter in PhysicalPlan (explain)
c_name_df.explain(True)
jane_df.explain(True)


# between doesn't work with PushDown filter
between_select = all_names_df.filter(F.col("cid").between('20b5aae6a3e31111009f9a7ecc31a232', 'b89010d4a6acaa06d4ef89043869838e'))
between_select.explain(True)
between_select.show()
between_select.count()


# in works with PushDown filter
in_select = all_names_df.filter(F.col("cid").isin('20b5aae6a3e31111009f9a7ecc31a232', 'b89010d4a6acaa06d4ef89043869838e'))
in_select.explain(True)
in_select.show()
in_select.count()





# Another READ CASSANDRA example #
# Pushed filter #
cass_big_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_many", keyspace="keyspace1") \
    .load()

between_select = cass_big_df.filter(F.col("user_id").between(4164237664, 4164237664+1) )
between_select.explain(True)
between_select.show()


in_select = cass_big_df.filter(F.col("user_id").isin(4164237664, 4164237664+1) )
in_select.explain(True)
in_select.show()
