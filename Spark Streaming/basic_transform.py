from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('WARN')	

#Please note that you will need to replace the Public IP placeholder below with the Public IP of your EMR Master Node. For example - ("host", "ec2-54-196-108-141.compute-1.amazonaws.com")	
lines = spark  \
	.readStream  \
	.format("socket")  \
	.option("host","<Public IP of MasterNode>")  \
	.option("port",12345)  \
	.load()
	
transformedDF = lines.filter(length(col("value"))>4)
	
query = transformedDF  \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.start()
	
query.awaitTermination()
