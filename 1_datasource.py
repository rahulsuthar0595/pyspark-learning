from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("DataSource").getOrCreate()

df_1 = spark.read.csv("/home/mind/Source/pyspark-python/data/zipcodes.csv", header=True)
df_1.printSchema()

print("Read multiple CSV file \n")
df_2 = spark.read.csv(
    path=["/home/mind/Source/pyspark-python/data/zipcodes.csv", "/home/mind/Source/pyspark-python/data/salaries.csv"],
    header=True
)
df_2.printSchema()

print("Read JSON file \n")
df_3 = spark.read.json(
    path=["/home/mind/Source/pyspark-python/data/user.json"],
    multiLine=True
)
df_3.printSchema()

print("Pyspark Write Options")
df_4 = spark.read.json(
    path=["/home/mind/Source/pyspark-python/data/user.json"],
    multiLine=True
)
# Mode: append, overwrite, error, errorifexists and ignore
# This will create a folder with json file for each partition
df_4.write.json("data/updated_user.json", mode="overwrite")

print("Read data from generated folder \n")
df_5 = spark.read.json("data/updated_user.json")
df_5.show()
