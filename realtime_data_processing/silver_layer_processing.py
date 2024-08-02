from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from delta.tables import *
def silver_layer_processing(spark, bronze_df):
    print("-------------------- SILVER LAYER PROCESSING --------------------")
    orders_df1 = bronze_df.selectExpr("CAST(value AS STRING)", "timestamp")
    print("[auxiliary] orders_df1: ")
    orders_df1.printSchema()
    # enforcing schema on data
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("order_product_name", StringType()) \
        .add("order_card_type", StringType()) \
        .add("order_amount", StringType()) \
        .add("order_datetime", StringType()) \
        .add("order_country_name", StringType()) \
        .add("order_city_name", StringType()) \
        .add("order_ecommerce_website_name", StringType())
    
    # aux_sil_del_table = DeltaTable.forPath(spark, '/home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/spark-warehouse/orders_silver')
    orders_df2 = orders_df1\
        .select(from_json(col("value"), orders_schema)
                .alias("orders"), "timestamp")
    
    print("[auxiliary] order_df2 schema: \n")
    orders_df2.printSchema()
    orders_df3 = orders_df2.select("orders.*", "timestamp")

    print("[auxiliary] Printing schema of orders_df3 before creating date & hour column from order_datetime: ")
    orders_df3.printSchema()

    orders_df3 = orders_df3.withColumn( 
        "partition_date", to_date("order_datetime"))
    orders_df3 = orders_df3.withColumn("partition_hour", hour(
        to_timestamp("order_datetime", 'yyyy-MM-dd HH:mm:ss')))

    #? silver_delta_table is a spark session or a delta lake session?
    #! --> delta table is updated to a dataframe that is under a streaming session?
        # save to delta lake
    
    print("[auxiliary] Printing schema of orders_df3 after creating date & hour column from order_datetime: ")
    orders_df3.printSchema()
    orders_df3.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "hdfs://localhost:9000/nguyenkieubaokhanh/apache-mock-project/orders_silver") \
        .toTable("orders_silver")
    
    return orders_df3
