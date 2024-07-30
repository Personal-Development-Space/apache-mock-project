This piece of code isn't working
```
    # (silver_delta_table.alias('silver') \
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
    #     # "source": "updates.source",
    #     # "is_flagged": "updates.is_flagged",
    #     # "created_at": "updates.created_at",
    #     # "modified_at": "updates.modified_at"
    #     }
    # ) \
    # .execute())
```

"""
-------------------- BRONZE LAYER PROCESSING --------------------
Printing Schema of orders_df: 
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)

-------------------- SILVER LAYER PROCESSING --------------------
[auxiliary] orders_df1: 
root
 |-- value: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)

order_df2 schema: 

root
 |-- orders: struct (nullable = true)
 |    |-- order_id: string (nullable = true)
 |    |-- order_product_name: string (nullable = true)
 |    |-- order_card_type: string (nullable = true)
 |    |-- order_amount: string (nullable = true)
 |    |-- order_datetime: string (nullable = true)
 |    |-- order_country_name: string (nullable = true)
 |    |-- order_city_name: string (nullable = true)
 |    |-- order_ecommerce_website_name: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)

Printing schema of orders_df3 before creating date & hour column from order_datetime: 
root
 |-- order_id: string (nullable = true)
 |-- order_product_name: string (nullable = true)
 |-- order_card_type: string (nullable = true)
 |-- order_amount: string (nullable = true)
 |-- order_datetime: string (nullable = true)
 |-- order_country_name: string (nullable = true)
 |-- order_city_name: string (nullable = true)
 |-- order_ecommerce_website_name: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)

Printing schema of orders_df3 after creating date & hour column from order_datetime: 
root
 |-- order_id: string (nullable = true)
 |-- order_product_name: string (nullable = true)
 |-- order_card_type: string (nullable = true)
 |-- order_amount: string (nullable = true)
 |-- order_datetime: string (nullable = true)
 |-- order_country_name: string (nullable = true)
 |-- order_city_name: string (nullable = true)
 |-- order_ecommerce_website_name: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- partition_date: date (nullable = true)
 |-- partition_hour: integer (nullable = true)

-------------------- GOLD LAYER PROCESSING --------------------
Printing Schema of orders_df4: 
root
 |-- order_card_type: string (nullable = true)
 |-- total_sales: double (nullable = true)

root
 |-- card_type: string (nullable = true)
 |-- total_sales: double (nullable = true)

Printing Schema of orders_df5: 
root
 |-- order_country_name: string (nullable = true)
 |-- total_sales: double (nullable = true)

root
 |-- country: string (nullable = true)
 |-- total_sales: double (nullable = true)
"""