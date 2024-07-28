from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

from configparser import ConfigParser
def bronze_layer_processing(spark, kafka_bootstrap_servers, input_kafka_topic_name):
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    print("-------------------- BRONZE LAYER PROCESSING --------------------")
    print("Printing Schema of orders_df: ")
    orders_df.printSchema()
    return orders_df