from pyspark.sql import SparkSession

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
	
query = lines  \
	.writeStream  \
	.outputMode("update")  \
	.format("console")  \
	.trigger(once=True)  \
	.start()
	
query.awaitTermination()
