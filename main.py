from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("Pyspark test").getOrCreate()

sc = spark.sparkContext

print(f"application id: {sc.applicationId}")
print(f"version: {sc.version}")
print(f"ui web url: {sc.uiWebUrl}")

print(f"sc -> {sc}")

df = spark.read.csv("data/salaries.csv", header=True)
df.show(truncate=False)

print(f"Head: {df.head(n=5)} \n")
print(f"Tail: {df.tail(num=5)} \n")

print(f"Columns: {df.columns} \n")

print(f"dtypes with columns: {df.dtypes} \n")

print(f"Get Results as list of Rows Collect: {df.collect()}")

dicts = [row.asDict(recursive=True) for row in df.collect()]
print(f"Get Results as list of dict: {dicts} \n")

pd_df = df.toPandas()
print(f"Pandas Dataframe: {pd_df} \n")

# Filter
df = df.filter(col("work_year").isin(2020, 2021, 2022))
# OR
# df = df.filter(df.work_year <= 2022)
# # OR
# df = df.filter((df.work_year > 2020) & (df.work_year < 2023))

print(f"Filter condition \n")
df.show(5)

print(f"Order By condition \n")
df.orderBy(df.salary.asc()).show(5)
df.orderBy(df.salary.desc()).show(5)

df = df.withColumn("status", lit("N/A")).withColumn("salary_with_increment", df.salary)
print("Static column add: \n")
df.show(5)


# Custom Functions
def increment(salary):
    return salary + ((salary * 10) / 100)


increment_udf = udf(increment)
df = df.withColumn("salary_with_increment", increment_udf(col("salary_with_increment").cast(IntegerType())))
print("User defined function \n")
df.show(5)

df = df.distinct()
df.show(10)

df = df.filter(col("job_title").contains("Data"))
df.show()
print(df.count())

df = df.withColumn("salary_with_increment", F.ceil("salary_with_increment"))
df.show(5)

df = df.groupby("job_title").agg(F.avg("salary").cast(IntegerType())).alias("avg_salary")
df.show(10, truncate=False)

df = df.drop()

spark.stop()
