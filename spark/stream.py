from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "testtopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")
    breakpoint()
    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars",
                "file:///home/mind/Source/pyspark-python/spark/spark-sql-kafka-0-10_2.12-3.2.0.jar,file:///home/mind/Source/pyspark-python/spark/kafka-clients-2.8.0.jar") \
        .getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("ERROR")

    # spark = SparkSession \
    #     .builder \
    #     .appName("PySpark Structured Streaming with Kafka Demo") \
    #     .master("local[*]") \
    #     .config("spark.jars",
    #             "file:///home/mind/Source/pyspark-python/spark/spark-sql-kafka-0-10_2.12-3.2.0.jar,file:///home/mind/Source/pyspark-python/spark/kafka-clients-2.8.0.jar") \
    #     .config("spark.executor.extraClassPath",
    #             "file:///home/mind/Source/pyspark-python/spark/spark-sql-kafka-0-10_2.12-3.2.0.jar:file:///home/mind/Source/pyspark-python/spark/kafka-clients-2.8.0.jar") \
    #     .config("spark.executor.extraLibrary",
    #             "file:///home/mind/Source/pyspark-python/spark/spark-sql-kafka-0-10_2.12-3.2.0.jar:file:///home/mind/Source/pyspark-python/spark/kafka-clients-2.8.0.jar") \
    #     .config("spark.driver.extraClassPath",
    #             "file:///home/mind/Source/pyspark-python/spark/spark-sql-kafka-0-10_2.12-3.2.0.jar:file:///home/mind/Source/pyspark-python/spark/kafka-clients-2.8.0.jar") \
    #     .getOrCreate()
    #
    # spark.sparkContext.setLogLevel("ERROR")
    # Read from Kafka topic
    breakpoint()
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()
    # Construct a streaming DataFrame that reads from testtopic
    # transaction_detail_df = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    #     .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
    #     .option("startingOffsets", "latest") \
    #     .load()

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    # Define schema for the value (transaction details)
    transaction_detail_schema = StructType() \
        .add("transaction_id", StringType()) \
        .add("transaction_card_type", StringType()) \
        .add("transaction_amount", StringType()) \
        .add("transaction_datetime", StringType())

    # transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")
    #
    # # Define a schema for the transaction_detail data
    # transaction_detail_schema = StructType() \
    #     .add("transaction_id", StringType()) \
    #     .add("transaction_card_type", StringType()) \
    #     .add("transaction_amount", StringType()) \
    #     .add("transaction_datetime", StringType())
    #
    # transaction_detail_df2 = transaction_detail_df1 \
    #     .select(from_json(col("value"), transaction_detail_schema).alias("transaction_detail"), "timestamp")
    #
    # transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")
    #
    # # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    # transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type") \
    #     .agg({'transaction_amount': 'sum'}).select("transaction_card_type", \
    #                                                col("sum(transaction_amount)").alias("total_transaction_amount"))
    #
    # print("Printing Schema of transaction_detail_df4: ")
    # transaction_detail_df4.printSchema()
    #
    # transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100)) \
    #     .withColumn("value", concat(lit("{'transaction_card_type': '"), \
    #                                 col("transaction_card_type"), lit("', 'total_transaction_amount: '"), \
    #                                 col("total_transaction_amount").cast("string"), lit("'}")))
    #
    # print("Printing Schema of transaction_detail_df5: ")
    # transaction_detail_df5.printSchema()
    #
    # # Write final result into console for debugging purpose
    # trans_detail_write_stream = transaction_detail_df5 \
    #     .writeStream \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false") \
    #     .format("console") \
    #     .start()
    #
    # # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    # trans_detail_write_stream_1 = transaction_detail_df5 \
    #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    #     .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("checkpointLocation", "file:///home/mind/Source/pyspark-python/py_checkpoint") \
    #     .start()
    # trans_detail_write_stream.awaitTermination()
    transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")
    transaction_detail_df2 = transaction_detail_df1 \
        .select(from_json(col("value"), transaction_detail_schema).alias("transaction_detail"), "timestamp")

    # Flatten the JSON structure
    transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

    # Aggregation: sum transaction amount by card type
    transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type") \
        .agg({'transaction_amount': 'sum'}).select("transaction_card_type", \
                                                   col("sum(transaction_amount)").alias("total_transaction_amount"))

    # Add a key-value pair for Kafka
    transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100)) \
        .withColumn("value", concat(lit("{'transaction_card_type': '"), \
                                    col("transaction_card_type"), lit("', 'total_transaction_amount: '"), \
                                    col("total_transaction_amount").cast("string"), lit("'}")))

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

    # Await termination for both streams
    trans_detail_write_stream.awaitTermination()
    trans_detail_write_stream_1.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")
