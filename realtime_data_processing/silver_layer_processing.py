from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from delta.tables import *

def silver_layer_processing(spark, bronze_df):
    print("-------------------- SILVER LAYER PROCESSING --------------------")
    orders_df1 = bronze_df.selectExpr("CAST(value AS STRING)", "timestamp")
    print("[auxiliary] orders_df1: ")
    orders_df1.printSchema()
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("order_product_name", StringType()) \
        .add("order_card_type", StringType()) \
        .add("order_amount", StringType()) \
        .add("order_datetime", StringType()) \
        .add("order_country_name", StringType()) \
        .add("order_city_name", StringType()) \
        .add("order_ecommerce_website_name", StringType())
    
    silver_delta_table = DeltaTable.createIfNotExists(spark) \
        .tableName("orders_silver") \
        .addColumn("order_id", StringType()) \
        .addColumn("order_product_name", StringType()) \
        .addColumn("order_card_type", StringType()) \
        .addColumn("order_amount", StringType()) \
        .addColumn("order_datetime", StringType()) \
        .addColumn("order_country_name", StringType()) \
        .addColumn("order_city_name", StringType()) \
        .addColumn("order_ecommerce_website_name", StringType()) \
        .addColumn("source", StringType()) \
        .addColumn("is_flagged", BooleanType()) \
        .addColumn("created_at", DateType()) \
        .addColumn("modified_at", DateType()) \
        .execute()
    
    # deltaTable = DeltaTable.forPath(spark, '/tmp/delta-table/orders_silver')
    
    orders_df2 = orders_df1\
        .select(from_json(col("value"), orders_schema)
                .alias("orders"), "timestamp")
    
    # df_updates = orders_df2
    # silver_delta_table.alias('silver') \
    # .merge(
    #     df_updates.alias('updates'),
    #     'silver.order_id = updates.order_id' #and silver.order_datetime = updates.order_datetime and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item
    # ) \
    # .whenMatchedUpdate(set ={}) \
    # .whenNotMatchedInsert(values =
    #     {
    #     "order_id": "updates.order_id",
    #     "order_product_name": "updates.order_product_name",
    #     "order_card_type": "updates.order_card_type",
    #     "order_amount": "updates.order_amount",
    #     "order_datetime": "updates.order_datetime",
    #     "order_country_name": "updates.order_country_name",
    #     "order_city_name": "updates.order_city_name",
    #     "order_ecommerce_website_name": "updates.order_ecommerce_website_name",
    #     "source": "updates.source",
    #     "is_flagged": "updates.is_flagged",
    #     "created_at": "updates.created_at",
    #     "modified_at": "updates.modified_at"
    #     }
    # ) \
    # .execute()
    
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
