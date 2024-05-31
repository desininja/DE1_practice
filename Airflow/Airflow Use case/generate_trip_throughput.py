from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, sum, when, lit

import sys

spark = SparkSession \
    .builder \
    .appName("generate trip throughput") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.shuffle.partitions", 5) \
    .enableHiveSupport().getOrCreate()

run_date = sys.argv[1]

trips = spark.table("events.trip").filter(expr("trip_dt = '{}'".format(run_date)))
bookings = spark.table("events.booking").filter(expr("booking_dt = '{}'".format(run_date)))
bookings.join(trips, "booking_id", "left_outer") \
    .groupBy("city", "booking_dt") \
    .agg(sum(when(expr("trip_id IS NULL"), 0).otherwise(1)).alias('total_trips'), sum(lit(1)).alias('total_bookings')) \
    .select("city", expr("total_trips / CAST(total_bookings AS DOUBLE)").alias("trip_throughput"), "booking_dt") \
    .createOrReplaceTempView("result")

insert_sql = """INSERT OVERWRITE TABLE events.trip_throughput
         PARTITION (booking_dt)
         SELECT * FROM result"""

spark.sql(insert_sql)