from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

import sys

spark = SparkSession \
    .builder \
    .appName("filter booking data") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.shuffle.partitions", 5) \
    .enableHiveSupport().getOrCreate()

run_date = sys.argv[1]

spark.table("events.booking_raw") \
    .filter(expr("booking_dt = '{}'".format(run_date))) \
    .filter(expr("car_type != 'null'")) \
    .createOrReplaceTempView("result")

insert_sql = """INSERT OVERWRITE TABLE events.booking
         PARTITION (booking_dt)
         SELECT * FROM result"""

spark.sql(insert_sql)