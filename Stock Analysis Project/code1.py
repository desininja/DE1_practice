from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

# command: 
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 code1.py
spark = SparkSession.builder.appName('abc1').master('local[1]').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

schema = StructType([
    StructField("symbol", StringType(), True), 
    StructField("timestamp", TimestampType(), True), 
    StructField("priceData", StructType([
        StructField("close", FloatType(), True), 
        StructField("high", FloatType(), True), 
        StructField("low", FloatType(), True), 
        StructField("open", FloatType(), True), 
        StructField("volume", FloatType(), True)
    ]), True)
    ])

df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "52.55.237.11:9092") \
  .option("subscribe", "stockData") \
  .option("auto.offset.reset","latest") \
  .load()



df = df.selectExpr('CAST(value AS STRING)') \
    .select(from_json('value', schema).alias("value" )) \
    .select("value.symbol","value.timestamp","value.priceData.high","value.priceData.low","value.priceData.open","value.priceData.close","value.priceData.volume")

df = df \
.withWatermark("timestamp", "1 minutes") \
.groupBy(
    df.symbol, 
    df.timestamp, window(df.timestamp, "10 minutes", "5 minutes")) \
    .agg(F.avg(df.close).alias("Moving Average"))


df.writeStream \
  .outputMode("update") \
  .format("console") \
  .option("truncate", "false") \
  .start() \
  .awaitTermination()

