from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, sum, lit, rank, desc
from pyspark.sql.window import Window

import sys

spark = SparkSession \
    .builder \
    .appName("car type with highest trips") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.shuffle.partitions", 5) \
    .enableHiveSupport().getOrCreate()

run_date = sys.argv[1]

trips = spark.table("events.trip").filter(expr("trip_dt = '{}'".format(run_date)))
bookings = spark.table("events.booking").filter(expr("booking_dt = '{}'".format(run_date)))
bookings.join(trips, "booking_id", "inner") \
    .groupBy("city", "car_type", "trip_dt") \
    .agg(sum(lit(1)).alias('num_trips')) \
    .withColumn("rank", rank().over(Window.partitionBy("city", "trip_dt").orderBy(desc("num_trips")))) \
    .filter(expr("rank = 1")) \
    .select("city", "car_type", "num_trips", "trip_dt") \
    .createOrReplaceTempView("result")

insert_sql = """INSERT OVERWRITE TABLE events.car_with_most_trips
         PARTITION (trip_dt)
         SELECT * FROM result"""

spark.sql(insert_sql)
