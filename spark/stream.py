# Write to console for debugging
trans_detail_write_stream = transaction_detail_df5 \
    .writeStream \
    .trigger(processingTime='1 seconds') \
    .outputMode("update") \
    .option("truncate", "false") \
    .format("console") \
    .start()

# Write to Kafka
trans_detail_write_stream_1 = transaction_detail_df5 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    .trigger(processingTime='1 seconds') \
    .outputMode("update") \
    .option("checkpointLocation", "file:///home/mind/Source/pyspark-python/py_checkpoint") \
    .start()
