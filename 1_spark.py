from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySparkTest").getOrCreate()
sc = spark.sparkContext

print(f"application id: {sc.applicationId}")
print(f"version: {sc.version}")
print(f"ui web url: {sc.uiWebUrl}")

print(f"sc -> {sc}")
spark.stop()
