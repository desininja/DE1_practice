from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	
	
lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("subscribe","test")  \
	.load()

kafkaDF = lines.selectExpr("cast(key as string)","cast(value as string)")	


query = kafkaDF  \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.start()
	
query.awaitTermination()
