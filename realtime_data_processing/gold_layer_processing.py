from pyspark.sql.functions import *
from pyspark.sql.types import *

def gold_layer_processing(spark, silver_df):
    print("-------------------- GOLD LAYER PROCESSING --------------------")
    # Simple aggregate - find total_sales(sum of order_amount) by order_card_type
    orders_df4 = silver_df.groupBy("order_card_type") \
        .agg({'order_amount': 'sum'}) \
        .select("order_card_type", col("sum(order_amount)")
                .alias("total_sales"))

    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()
    

    orders_df4 = orders_df4.withColumnRenamed("order_card_type", "card_type")

    orders_df4.printSchema()
    orders_df4.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/spark-warehouse/orders_gold/by_card_type") \
        .toTable("by_card_type")
    # Simple aggregate - find total_sales(sum of order_amount) by order_country_name
    orders_df5 = silver_df.groupBy("order_country_name") \
        .agg({'order_amount': 'sum'}) \
        .select("order_country_name", col("sum(order_amount)")
                .alias("total_sales"))

    print("Printing Schema of orders_df5: ")
    orders_df5.printSchema()

    orders_df5 = orders_df5.withColumnRenamed("order_country_name", "country")

    orders_df5.printSchema()
    orders_df5.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/spark-warehouse/orders_gold/by_country") \
        .toTable("by_country")
    return [orders_df4, orders_df5]