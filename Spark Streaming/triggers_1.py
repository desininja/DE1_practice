from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Triggers") \
        .getOrCreate()

lines = spark.readStream \
        .format("Socket") \
        .option("host","localhost") \
        .option("port", 9999) \
        .load()


query = lines.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="4 seconds") \
        .start()


query.awaitTermination()