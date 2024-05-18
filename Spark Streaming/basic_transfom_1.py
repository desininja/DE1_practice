from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Basic Transformations").getOrCreate()

lines = spark \
        .readStream \
        .format("Socket") \
        .option("host","localhost") \
        .option("port",9999) \
        .load()


transformDF = lines.filter(length(col('value'))>4)

query = transformDF \
        .writeStream \
        .format('console') \
        .outputMode('append') \
        .start()

query.awaitTermination()