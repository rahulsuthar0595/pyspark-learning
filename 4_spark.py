"""
PySpark SQL Functions for JSON
"""
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object, json_tuple, col

spark = SparkSession.builder.master("local").appName("PySparkJsonFunction").getOrCreate()

data_1 = {"data": [
    {"dept": "Finance", "rank": 10}, {"dept": "Marketing", "rank": 20}, {"dept": "Sales", "rank": 30},
    {"dept": "IT", "rank": 40}]
}
json_data_1 = json.dumps(data_1)

df_1 = spark.createDataFrame([(json_data_1,)], ["json_data"])

df_1.show(truncate=False)

print("Json Get Object \n")
df_1.select(
    get_json_object("json_data", "$.data").alias("data")
).show(truncate=False)

print("Json Get Object From List \n")
df_1.select(
    get_json_object("json_data", "$.data[0]").alias("first_data")
).show(truncate=False)

df_1.select(
    get_json_object("json_data", "$.data[0].dept").alias("dept")
).show(truncate=False)

print("Json Tuple with each column \n")
df_1.select(
    get_json_object("json_data", "$.data[0].dept").alias("dept")
)

data_2 = {"dept": "Finance", "rank": 10}
json_data_2 = json.dumps(data_2)

df_2 = spark.createDataFrame([(json_data_2,)], ["json_data"])

print("Json tuple \n")
df_2.select(
    json_tuple(col("json_data"), "dept", "rank")
).toDF("dept", "rank").show()
