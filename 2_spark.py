from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# master specifies, the cluster manager or deployment mode i.e local, yarn, spark ip, mesos, kubernates
# Here local[n], specifies the CPU cores uses. Local is useful for testing, debugging or small-scale development.
spark = SparkSession.builder.master("local[1]").appName("PySparkTest").getOrCreate()

data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
print("Default Partition Count: ", str(rdd.getNumPartitions()))

rdd = spark.sparkContext.parallelize(data, 2)
print("Manual Partition Count: ", str(rdd.getNumPartitions()))
print("Manual Partition: ", rdd.glom().collect())

empty_rdd = spark.sparkContext.emptyRDD()
print(f"empty rdd: {empty_rdd}")

new_rdd = spark.sparkContext.parallelize([])
print(f"New RDD with partition: {new_rdd}")

# RDD to Dataframe

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
rdd_1 = spark.sparkContext.parallelize(dept)

print(f"RDD 1 Data: {rdd_1.collect()}")

schema = ["Department", "Number"]
df = rdd_1.toDF(schema=schema)
df.printSchema()
print(df.show(truncate=False))

# Alternate of above

df = spark.createDataFrame(rdd_1, schema=schema)
df.printSchema()
print(df.show(truncate=False))

pandas_df = df.toPandas()
print(f"Pyspark to pandas dataframe: \n{pandas_df}")

# Nested data into struct dataframe

data_struct = [
    (("James", "", "Smith"), "36636", "M", "3000"),
    (("Michael", "Rose", ""), "40288", "M", "4000"),
    (("Robert", "", "Williams"), "42114", "M", "4000"),
    (("Maria", "Anne", "Jones"), "39192", "F", "4000"),
    (("Jen", "Mary", "Brown"), "", "F", "-1")
]

struct_schema = StructType([
    StructField(
        "name", StructType([
            StructField("first_name", StringType(), False),
            StructField("middle_name", StringType(), True),
            StructField("last_name", StringType(), False),
        ]),
    ),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True),
])

df = spark.createDataFrame(data=data_struct, schema=struct_schema)
print(f"Nested structure data schema: \n{df.printSchema()}")
print(f"Nested structure data pandas: \n{df.toPandas()}")

# Show arguments

print(f"Show without argument: \n")
df.show()  # Default to 20

print(f"Show with truncate: \n")
df.show(2, truncate=5)

print("Show with vertical True: \n")
df.show(vertical=True)

# Modify Dataframe PySpark.

updated_df = df.withColumn(
    "other_info",
    struct(
        col("id").alias("identifier"),
        col("gender").alias("gender"),
        col("salary").alias("salary"),
        when(
            col("salary").cast(IntegerType()) < 2000, "Low"
        ).when(
            col("salary").cast(IntegerType()) < 4000, "Average"
        ).otherwise(
            "High"
        ).alias("salary_grade")
    )
).drop("id", "gender", "salary")
print("Updated dataframe: \n")
updated_df.printSchema()
updated_df.show(truncate=False)

# Select Dataframe

updated_df.select(updated_df.name.first_name, updated_df.other_info).show(truncate=False)

updated_df.select("name.first_name", "other_info").show(2, truncate=False)
updated_df.select("name.*").show(2, truncate=False)

updated_df.select([col for col in updated_df.columns]).show(truncate=False)
# OR
updated_df.select("*").show(truncate=False)

# Collect()

data = [1, 2, 3, 4, 5]
rdd_2 = spark.sparkContext.parallelize(data, 2)
result_rdd_2 = rdd_2.map(lambda x: x * 2)
print(f"Result rdd2 glom collect : {result_rdd_2.glom().collect()}")
print(f"Result rdd2 collect: {result_rdd_2.collect()}")
print(f"Original rdd2 collect: {rdd_2.collect()}")

# Transform data using WithColumn

df_1 = df.withColumn(
    "salary", col("salary").cast(IntegerType()) * 12
)
print(f"Multiply Column : \n")
df_1.show()

df_2 = df.withColumnRenamed("salary", "ctc")
print("Renamed colum: \n")
df_2.show()


# Filter Dataframe



spark.stop()
