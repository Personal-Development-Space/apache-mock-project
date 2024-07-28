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
    print("Printing Schema of orders_df: ")
    orders_df.printSchema()
    return orders_df
