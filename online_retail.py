from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, TimestampType, FloatType
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("OnlineRetail").getOrCreate()

schema = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", StringType()),
    StructField("UnitPrice", FloatType()),
    StructField("CustomerID", IntegerType()),
    StructField("Country", StringType()),
])

# 1. Define Schema Manually
df = spark.read.csv("data/Online_Retail.csv", header=True, schema=schema)
df.printSchema()
df.show(5)

# 2. Create a new column that represents the total price for each item
df = df.withColumn(
    "TotalPrice", F.col("Quantity") * F.col("UnitPrice")
).withColumn(
    "TotalPrice", F.round("TotalPrice", 2)
)
df.show(5)

# 3. Convert the InvoiceDate from string to a proper datetime format and extract a separate Date and Time columns
spark.conf.set("spark.sql.legacy.timeParserPolicy" ,"LEGACY")

df = df.withColumn(
    "InvoiceDate", to_timestamp("InvoiceDate", "MM/dd/yyyy H:mm")
).withColumn(
    "Date", F.to_date("InvoiceDate")
).withColumn(
    "Time", F.date_format("InvoiceDate", "HH:mm")
)

df.show(5)

# 4. Calculate the total revenue per day and per country

df_1 = df.groupby("Country", "Date").agg(F.sum("TotalPrice").alias("TotalRevenuePerDay")).sort("Country")
df_1 = df_1.withColumn("TotalRevenuePerDay", F.round("TotalRevenuePerDay", 2)).sort(col("Country").asc(), col("Date").asc())
df_1.show()

# 5. What country do most purchases come from
df_2 = df_1.groupby("Country").agg(F.sum("TotalRevenuePerDay").alias("TotalRevenue")).sort("TotalRevenue", ascending=False)
df_2.show()

# 6. Top Selling Products per country
df_3 = df.groupby("Description", "Country").agg(F.count("Description").alias("TotalQuantitySold"))
window_spec = Window.partitionBy("Country").orderBy(F.col("TotalQuantitySold").desc())
df_3 = df_3.withColumn("TopProduct", F.dense_rank().over(window_spec))
df_3.select("Country", "Description").where(F.col("TopProduct") == 1).show()


# 7. Find the total amount spent by each customer
df_4 = df.groupby("CustomerID").agg(F.sum("TotalPrice").alias("TotalAmountSpend"))
df_4 = df_4.withColumn("TotalAmountSpend", F.round("TotalAmountSpend", 2)).sort("TotalAmountSpend", ascending=[False])
df_4.show()

# 8. When was the earliest purchase made by a customer on the e-commerce platform
df_5 = df.select(
    "CustomerID", col("InvoiceDate").alias("EarliestPurchase")
).sort("EarliestPurchase", ascending=[True])
df_5.show(1)

# 9. Analyze the purchase frequency by hour of day and identify peak shopping hours
df_6 = df.withColumn("Hour", F.date_format("InvoiceDate", "HH"))
df_6 = df_6.groupby("Hour").agg(F.count("Hour").alias("HourFreq")).sort("HourFreq")
df_6.show()
