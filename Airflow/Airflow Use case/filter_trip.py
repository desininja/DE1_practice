from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

import sys

spark = SparkSession \
    .builder \
    .appName("filter trip data") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.shuffle.partitions", 5) \
    .enableHiveSupport().getOrCreate()

run_date = sys.argv[1]

spark.table("events.trip_raw") \
    .filter(expr("trip_dt = '{}'".format(run_date))) \
    .filter(expr("pickup_location_id != dropoff_location_id")) \
    .createOrReplaceTempView("result")

insert_sql = """INSERT OVERWRITE TABLE events.trip
         PARTITION (trip_dt)
         SELECT * FROM result"""

spark.sql(insert_sql)
