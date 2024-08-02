from delta import *
from delta.tables import *

def bronze_layer_processing(spark, kafka_bootstrap_servers, input_kafka_topic_name):
    print("-------------------- BRONZE LAYER PROCESSING --------------------")
    # failOnDataLoss = false --> turn off the warning
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    # raw data is save to delta lake under delta format
    orders_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "hdfs://localhost:9000/nguyenkieubaokhanh/apache-mock-project/orders_bronze") \
        .toTable("orders_bronze") \
    # DeltaTable.isDeltaTable(spark, "spark-warehouse/orders_bronze")
    print("[auxiliary] Printing Schema of orders_df: ")
    orders_df.printSchema()
    print("[auxiliary] reading delta file... ")
    aux_df = spark.read.format("delta").load("hdfs://localhost:9000/nguyenkieubaokhanh/apache-mock-project/orders_bronze")
    aux_df.show()
    return orders_df
