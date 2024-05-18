from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Stream-stream-join").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
stream1 = spark \
            .readStream \
            .format('socket') \
            .option('host','localhost') \
            .option('port',9999) \
            .load()

streamDF1 = stream1.selectExpr("value as player")

stream2 = spark \
            .readStream \
            .format('socket') \
            .option('host','localhost') \
            .option('port',9998) \
            .load()

streamDF2 = stream2.selectExpr("value as person")

joinedStreamDF = streamDF1.join(streamDF2,expr("""player = person"""))

query = joinedStreamDF \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start()

query.awaitTermination()