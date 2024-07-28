from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from bronze_layer_processing import bronze_layer_processing
from silver_layer_processing import silver_layer_processing
from gold_layer_processing import gold_layer_processing
import time

from configparser import ConfigParser

# Loading Kafka Cluster/Server details from configuration file(datamaking_app.conf)
# Build a config object
conf_file_path = "/home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/"
conf_file_name = conf_file_path + "datamaking_app.conf"
config_obj = ConfigParser()
print(config_obj)
print(config_obj.sections())
config_read_obj = config_obj.read(conf_file_name)
print(type(config_read_obj))
print(config_read_obj)
print(config_obj.sections())

# Kafka Cluster/Server Details
kafka_host_name = config_obj.get('kafka', 'host')
kafka_port_no = config_obj.get('kafka', 'port_no')
input_kafka_topic_name = config_obj.get('kafka', 'input_topic_name')
output_kafka_topic_name = config_obj.get('kafka', 'output_topic_name')
kafka_bootstrap_servers = kafka_host_name + ':' + kafka_port_no

# MySQL Database Server Details
mysql_host_name = config_obj.get('mysql', 'host')
mysql_port_no = config_obj.get('mysql', 'port_no')
mysql_user_name = config_obj.get('mysql', 'username')
mysql_password = config_obj.get('mysql', 'password')
mysql_database_name = config_obj.get('mysql', 'db_name')
mysql_driver = config_obj.get('mysql', 'driver')

# get tables from `ecom_db`
mysql_salesbycardtype_table_name = config_obj.get(
    'mysql', 'mysql_salesbycardtype_tbl')
mysql_salesbycountry_table_name = config_obj.get(
    'mysql', 'mysql_salesbycountry_tbl')

mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + \
    ":" + mysql_port_no + "/" + mysql_database_name
# https://mvnrepository.com/artifact/mysql/mysql-connector-java
# --packages mysql:mysql-connector-java:5.1.49

# spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:5.1.49 --files /home/datamaking/workarea/code/course_download/ecom-real-time-case-study/realtime_data_processing/datamaking_app.conf /home/datamaking/workarea/code/course_download/ecom-real-time-case-study/realtime_data_processing/realtime_data_processing.py

# Create the Database properties
db_properties = {}
db_properties['user'] = mysql_user_name
db_properties['password'] = mysql_password
db_properties['driver'] = mysql_driver
# Java Database Connectivity (JDBC) is an application programming interface (API) for the Java programming language which defines how a client may access a database. It is a Java-based data access technology used for Java database connectivity.


def save_to_mysql_table(current_df, epoc_id, mysql_table_name):
    print("Inside save_to_mysql_table function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing mysql_table_name: " + mysql_table_name)

    mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + \
        ":" + str(mysql_port_no) + "/" + mysql_database_name

    current_df = current_df.withColumn('batch_no', lit(epoc_id))

    # Save the dataframe to the table.
    current_df.write.jdbc(url=mysql_jdbc_url,
                          table=mysql_table_name,
                          mode='append',
                          properties=db_properties)

    print("Exit out of save_to_mysql_table function")


if __name__ == "__main__":
    print("Real-Time Data Processing Application Started...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    extra_packages=["io.delta:delta-core_2.12:2.1.0"]
    builder = SparkSession \
        .builder \
        .appName("Real-Time Data Processing with Kafka Source and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        # .getOrCreate()
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    # io.delta:delta-core_2.12:2.1.0
    """
    spark-submit --master "local[*]" 
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:5.1.49 
    --files /home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/datamaking_app.conf 
            /home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/realtime_data_processing.py
    """

    """
    /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b06-1.fc39.x86_64/jre/bin/java -cp /home/nguyenkieubaokhanh/.local/lib/python3.12/site-packages/pyspark/conf:/home/nguyenkieubaokhanh/.local/lib/python3.12/site-packages/pyspark/jars/* -Xmx1g -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false 
    org.apache.spark.deploy.SparkSubmit 
    --master 
        local[*] 
    --packages 
        org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,
        mysql:mysql-connector-java:5.1.49 
    --files 
        /home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/datamaking_app.conf 
        /home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/realtime_data_processing.py
    """

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    # What is a bootstrap server: https://stackoverflow.com/questions/61656223/what-is-bootstrap-server-in-kafka-config

    bronze_df = bronze_layer_processing(spark, kafka_bootstrap_servers, input_kafka_topic_name)
    # key, value, topic, partition, offset, timestamp


    # Define a schema for the orders data --> usage of schema registry?
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    # {'order_id': 1, 'order_product_name': 'Laptop', 'order_card_type': 'MasterCard',
    # 'order_amount': 38.48, 'order_datetime': '2020-10-21 10:59:10', 'order_country_name': 'Italy',
    # 'order_city_name': 'Rome', 'order_ecommerce_website_name': 'www.flipkart.com'}
    silver_df = silver_layer_processing(bronze_df)

    orders_agg_write_stream_pre = silver_df \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    orders_agg_write_stream_pre_hdfs = silver_df.writeStream \
        .trigger(processingTime='10 seconds') \
        .format("parquet") \
        .option("path", "/tmp/data/ecom_data/raw") \
        .option("checkpointLocation", "orders-agg-write-stream-pre-checkpoint") \
        .partitionBy("partition_date", "partition_hour") \
        .start()

    [orders_df4, orders_df5] = gold_layer_processing(silver_df)

    orders_df4 \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("update") \
        .foreachBatch(lambda current_df, epoc_id: save_to_mysql_table(current_df, epoc_id, mysql_salesbycardtype_table_name)) \
        .start()

    orders_df5 \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("update") \
        .foreachBatch(lambda current_df, epoc_id: save_to_mysql_table(current_df, epoc_id, mysql_salesbycountry_table_name)) \
        .start()

    # Write final result into console for debugging purpose
    orders_agg_write_stream = orders_df4 \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    '''
    kafka_orders_df4 = orders_df4.selectExpr("card_type as key",
                                                 """to_json(named_struct(
                                                 'card_type', card_type,
                                                 'total_sales', total_sales)) as value""")

    # kafka_orders_df4 [key, value]
    kafka_writer_query = kafka_orders_df4 \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .queryName("Kafka Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", output_kafka_topic_name) \
        .outputMode("update") \
        .option("checkpointLocation", "kafka-check-point-dir") \
        .start()
    '''

    orders_agg_write_stream.awaitTermination()

    print("Real-Time Data Processing Application Completed.")
