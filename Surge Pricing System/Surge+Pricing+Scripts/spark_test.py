from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json
import pygeohash as gh
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, unix_timestamp, round


spark = SparkSession.builder \
  .master("local[*]") \
  .appName("drive-streaming") \
  .getOrCreate()


schema = StructType([
    StructField("id", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True), 
    StructField("ts", TimestampType(), True), 
    StructField("type", StringType(), True)
    ])

df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "surge_pricing_demo") \
  .option("startingOffsets","earliest") \
  .load()


df = df.selectExpr('CAST(value AS STRING)', "timestamp as ts") \
    .select(from_json('value', schema).alias("value" ), "ts") \
    .select("value.id","value.lat","value.long","value.ts","value.type")


def to_geohash(lat, long):
   #return "123"
   return gh.encode(float(lat), float(long), precision=5)

to_geohash_udf = udf(to_geohash,StringType())

def to_num(value, compare_to):
    return 1 if compare_to in value else 0

to_num_udf = udf(to_num, IntegerType())


df = df.withColumn("geohash", to_geohash_udf('lat', 'long') )

df = df.withColumn("drivers", to_num_udf('type', F.lit('DRIVER')) )

df = df.withColumn("customers", to_num_udf('type', F.lit('CUSTOMER')) )


df = df \
.withWatermark("ts", "1 minutes") \
.groupBy(
    window("ts", "1 minutes", "1 minutes"), df.geohash) \
   .agg(F.sum(df.drivers).alias("driver_sum"), F.sum(df.customers).alias("customer_sum") )



df = df.withColumn("surge/ride", col("customer_sum") / col("driver_sum") ).select("window","geohash","surge/ride")


df = df.select(to_json(F.struct("window","geohash","surge/ride")).alias("value"))

df.writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "surge_pricing_demo_output") \
  .option("checkpointLocation", "/tmp/surge_pricing_demo") \
  .start() \
  .awaitTermination()
