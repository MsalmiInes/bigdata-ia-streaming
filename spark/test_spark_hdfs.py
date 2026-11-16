"""Test phase 2 : cluster Spark + lecture/ecriture HDFS."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("phase2-smoke-test").getOrCreate()
print("Spark", spark.version, "| master :", spark.sparkContext.master)

df = spark.range(0, 1_000_000).selectExpr("id", "id % 10 AS bucket")
res = df.groupBy("bucket").count().orderBy("bucket")
res.show()

path = "hdfs://namenode:9000/tmp/spark_smoke"
res.write.mode("overwrite").parquet(path)
back = spark.read.parquet(path)
print("Lignes relues depuis HDFS :", back.count())
spark.stop()
