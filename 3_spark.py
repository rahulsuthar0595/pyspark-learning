"""
PySpark SQL Functions for Date and Timestamp
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, date_format, datediff, hour, minute, second, trunc, year, month, \
    next_day, weekofyear, current_timestamp, to_timestamp

spark = SparkSession.builder.master("local").appName("PySparkTest").getOrCreate()

data_1 = [["1", "2024-08-04"], ["2", "2024-09-21"], ["3", "2024-10-09"]]
df_1 = spark.createDataFrame(data_1, ["id", "date"])
df_1.show()

print("Return current date in YYYY-MM-DD format \n")
df_1.select(current_date().alias("current_date")).show(1)

print("Return date column along with new extra column date_format with specified date format \n")
df_1.select(
    col("date"),
    date_format(col("date"), "dd-MM-yyyy").alias("date_format")
).show()

print("Return difference between dates in days \n")
df_1.select(
    col("date"),
    datediff(current_date(), col("date")).alias("datediff")
).show()

print("Truncate date \n")
df_1.select(
    col("date"),
    trunc(col("date"), "year").alias("Year"),  # Truncate date to start of year, 2024-08-04 to 2024-01-01
    trunc(col("date"), "month").alias("Month"),  # Truncate date to start of the month, 2024-09-21 to 2024-09-01
    trunc(col("date"), "week").alias("Week"),
    # Truncate date to start of the week for that day, 2024-09-21 to 2024-09-16
    trunc(col("date"), "quarter").alias("Quarter"),  # Truncate date to start of the quarter, 2024-09-21 to 2024-07-01
).show()

print("Return Year, Month, Next day and Week of year \n")
df_1.select(
    col("date"),
    year(col("date")).alias("Year"),
    month(col("date")).alias("Month"),
    next_day(col("date"), "Sun").alias("Next Sunday"),
    weekofyear(col("date")).alias("Week Of Year"),
).show()

print("Current Timestamp \n")
df_1.select(
    current_timestamp().alias("CurrentTimestamp")
).show(1, truncate=False)

data_2 = [["1", "2020-02-01"], ["2", "2019-03-01 12:01:19.406"], ["3", "2021-03-01 12:01:19.406"]]
df_2 = spark.createDataFrame(data_2, ["id", "datetime"])

print("Timestamp change to format \n")
df_2.select(
    col("datetime"),
    to_timestamp(col("datetime")).alias("to_timestamp")
).show(truncate=False)

print("Hour, Minute and Second for Datetime: \n")
df_2.select(
    col("datetime"),
    hour(col("datetime")).alias("hour"),
    minute(col("datetime")).alias("minute"),
    second(col("datetime")).alias("second"),
).show(truncate=False)
