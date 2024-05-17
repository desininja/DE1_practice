from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("STream practice").getOrCreate()

lines = spark.readStream.format('socket').option('host','localhost').option('port','9999').load()

query = lines.writeStream.outputMode('complete').format('console').start()

query.awaitTermination()