from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

spark = SparkSession.builder \
        .appName('Static Stream join') \
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')	

stream = spark \
            .readStream \
            .format('socket') \
            .option('host','localhost') \
            .option('port',9999) \
            .load()


streamDF = stream.select(col('value').alias("player"))


mySchema = StructType().add('name','string').add('age','integer')
staticDF = spark.read.format('csv')\
                    .option('delimiter',';')\
                    .schema(mySchema)\
                    .load('/Users/himanshu/Desktop/Upgrad Revision/DE1_practice/Spark Streaming/players_info.csv')

joinedDF = streamDF.join(staticDF,expr("""player = name"""))

query = joinedDF \
            .writeStream \
            .outputMode('append') \
            .format('console') \
            .start()

query.awaitTermination()