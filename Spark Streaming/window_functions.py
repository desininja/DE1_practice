from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('WARN')	
	
mySchema = StructType().add("activity_type", "string").add("activity_time","timestamp").add("activity_count","integer")

lines = spark  \
	.readStream  \
	.schema(mySchema)  \
	.json("data/")

windowDF = lines.groupBy(window("activity_time","1 day","1 hour")).sum("activity_count").alias("Events_Sum").orderBy(asc("window"))
	
query = windowDF  \
	.writeStream  \
	.outputMode("complete")  \
	.format("console")  \
	.option("truncate", "False")  \
	.option("numRows",200)  \
	.start()
	
query.awaitTermination()
