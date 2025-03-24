from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_NAME = "testtopic"
scala_version = "2.12"
spark_version = "3.2.1"

schema = StructType(
    [
        StructField("work_year", IntegerType(), True),
        StructField("experience_level", StringType(), True),
        StructField("employment_type", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("salary_currency", StringType(), True),
        StructField("salary_in_usd", IntegerType(), True),
        StructField("employee_residence", StringType(), True),
        StructField("remote_ratio", IntegerType(), True),
        StructField("company_location", StringType(), True),
        StructField("company_size", StringType(), True),
    ]
)


def write_to_db(batch_df, batch_id):
    # Write to PostgreSQL
    batch_df.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/salaries_pyspark"
    ).option("dbtable", "salaries").option("user", "postgres").option(
        "password", "postgres"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()


if __name__ == "__main__":
    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
        f"org.apache.spark:spark-streaming-kafka-0-10_{scala_version}:{spark_version}",
        f"org.apache.spark:spark-token-provider-kafka-0-10_{scala_version}:{spark_version}",
        "org.apache.kafka:kafka-clients:2.1.1",
        "org.apache.commons:commons-pool2:2.8.0",
        "org.postgresql:postgresql:42.6.0",
    ]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PySparkKafka")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Read Data from Kafka Topic
    spark_data = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("includeHeaders", "true") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse Kafka JSON Messages
    salaries_df = (
        spark_data.selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), schema).alias("data"))
        .select("data.*")
    )

    # Filter High Salary Transactions (Salary > 100000)
    high_salaries = salaries_df.filter(col("salary") > 100000)

    # Checkpoint location to store metadata (needed for recovery after failure)
    checkpoint_path = "file:////home/mind/Source/pyspark-python/pyspark_kafka/py_checkpoint"

    # Writing to Console (for debugging) with Trigger Interval
    # Append mode: only new rows are written
    # format (console): Writing to console for debugging
    # Trigger every 5 seconds
    # Checkpointing enabled

    console_query = high_salaries.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .option("checkpointLocation", checkpoint_path) \
        .start()
    console_query.awaitTermination()  # Keeps the stream running

    # Writing to PostgreSQL with foreachBatch

    # Writing to PostgreSQL with foreachBatch
    # Writing each batch to PostgreSQL
    # Append mode: Only new data gets written
    # Process new data every 2 seconds
    # Checkpointing enabled



    db_query = high_salaries.writeStream \
        .foreachBatch(write_to_db) \
        .outputMode("append") \
        .trigger(processingTime="2 seconds") \
        .option("checkpointLocation", checkpoint_path) \
        .start()
    db_query.awaitTermination()  # Keeps the stream running
