from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, dense_rank, col, avg, sum, min, max, row_number, count, \
    first, approx_count_distinct, collect_list, collect_set, last
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local").appName("PySparkWindowFunc").getOrCreate()

data_1 = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
]

df_1 = spark.createDataFrame(data_1, ["name", "dept", "salary"])
df_1.show(truncate=False)

window_spec_1 = Window.partitionBy("dept").orderBy("salary")

print("Row number by dept : \n")
df_1.withColumn(
    "row", row_number().over(window_spec_1)
).show(truncate=False)

print(
    "Rank number by dept with salary. If same salary then give same number for that row and skip the gap for next : \n")
df_1.withColumn(
    "rank", rank().over(window_spec_1)
).show(truncate=False)

print(
    "Dense Rank number by dept with salary. If same salary then give same number for that row and without skip the gap for next : \n")
df_1.withColumn(
    "dense_rank", dense_rank().over(window_spec_1)
).show(truncate=False)

# Aggregate Functions
print("Aggregate functions \n")

window_spec_2 = Window.partitionBy("dept")  # For aggregate function, use window function without order
df_1.withColumn(
    "row", row_number().over(window_spec_1)
).withColumn(
    "avg", avg("salary").over(window_spec_2)
).withColumn(
    "max", max("salary").over(window_spec_2)
).withColumn(
    "min", min("salary").over(window_spec_2)
).withColumn(
    "sum", sum("salary").over(window_spec_2)
).withColumn(
    "count", count("salary").over(window_spec_2)
).where(
    col("row") == 1
).select("dept", "avg", "max", "min", "sum", "count").show(truncate=False)

print("first function \n")
df_1.select(first("salary").alias("First Salary")).show(truncate=False)

print("last function \n")
df_1.select(last("salary").alias("Last Salary")).show(truncate=False)

print("count of distinct value \n")
df_1.select(approx_count_distinct("salary")).show(truncate=False)

print("all values of an column with duplicates \n")
df_1.select(collect_list("salary")).show(truncate=False)

print("all values of an column without duplicates \n")
df_1.select(collect_set("salary")).show(truncate=False)
