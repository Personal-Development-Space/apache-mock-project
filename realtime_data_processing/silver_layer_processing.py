from pyspark.sql.functions import *
from pyspark.sql.types import *

def silver_layer_processing(bronze_df):
    print("-------------------- SILVER LAYER PROCESSING --------------------")

    orders_df1 = bronze_df.selectExpr("CAST(value AS STRING)", "timestamp")

    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("order_product_name", StringType()) \
        .add("order_card_type", StringType()) \
        .add("order_amount", StringType()) \
        .add("order_datetime", StringType()) \
        .add("order_country_name", StringType()) \
        .add("order_city_name", StringType()) \
        .add("order_ecommerce_website_name", StringType())
    
    orders_df2 = orders_df1\
        .select(from_json(col("value"), orders_schema)
                .alias("orders"), "timestamp")
    
    print("order_df2 schema: \n")
    orders_df2.printSchema()
    
    orders_df3 = orders_df2.select("orders.*", "timestamp")

    print("Printing schema of orders_df3 before creating date & hour column from order_datetime: ")
    orders_df3.printSchema()

    orders_df3 = orders_df3.withColumn(
        "partition_date", to_date("order_datetime"))
    orders_df3 = orders_df3.withColumn("partition_hour", hour(
        to_timestamp("order_datetime", 'yyyy-MM-dd HH:mm:ss')))

    print("Printing schema of orders_df3 after creating date & hour column from order_datetime: ")
    orders_df3.printSchema()
    return orders_df3
