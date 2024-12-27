from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from es_connect import ESConnect
from pyspark import SparkConf




class SparkWithES:
    def es_func(self):
        es = ESConnect("http://localhost:9200")
        breakpoint()
        conf = SparkConf()
        conf.set("spark.driver.extraClassPath", "/opt/spark/jars/elasticsearch-hadoop-8.17.0.jar")
        conf.set("es.nodes.discovery", "true")
        conf.set("es.node.data.only", "false")
        conf.set("es.nodes.wan.only", "false")
        conf.set("es.nodes.client.only", "true")
        conf.set("es.nodes",
                 "localhost:9200")
        conf.set("es.net.http.auth.user",
                 "spark")  # authorized user to read indexes. If you dont have any auth mechanism. You don't need this.
        conf.set("es.net.http.auth.pass", "youruserpassword")  # users password

        spark = SparkSession.builder.master("local").appName("SparkWithES").config(conf=conf).getOrCreate()
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
        df = spark.read.csv("data/Online_Retail.csv", schema=schema)

        print("Count", df.count())

        es.write("census_index", df)

        result = es.select(spark, "census_index", df, {"code": [541112, 271213]})

        result.show(truncate=False)


if __name__ == "__main__":
    SparkWithES().es_func()
