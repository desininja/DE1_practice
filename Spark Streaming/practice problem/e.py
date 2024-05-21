#command to run file: spark-submit Name of file(xyz.py)
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('abc').getOrCreate()


df = spark.readStream \
  .format("rate") \
  .option("rowsPerSecond", 1) \
  .load()


df = df.selectExpr("value","timestamp")

df = df.groupBy(
    window("timestamp", "1 minutes", "30 seconds")
).agg(F.count("value"))


df.writeStream \
  .outputMode("update") \
  .format("console") \
  .option("truncate", "false") \
  .start() \
  .awaitTermination()