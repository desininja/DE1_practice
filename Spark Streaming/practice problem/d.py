#command to run file: spark-submit Name of file(xyz.py)
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('abc').getOrCreate()
#Date,Description,Deposits,Withdrawls,Balance
schema = StructType([
    StructField("Date", StringType(), True), 
    StructField("Description", StringType(), True), 
    StructField("Deposits", StringType(), True), 
    StructField("Withdrawls", StringType(), True), 
    StructField("Balance", StringType(), True) 
    ])

transaction_type_json = '[{"type":"Tax","t_type":"offline"},{"type":"Cash","t_type":"offline"},{"type":"NEFT","t_type":"online"}]'

static_df = spark.read.json(spark.sparkContext.parallelize([transaction_type_json]))

path = "./data/*"
df = spark.readStream \
  .format("csv") \
  .option("maxFilesPerTrigger","1") \
  .schema(schema) \
  .load(path)

df = df.withColumn("Deposits",regexp_replace(df.Deposits,",","").cast(DoubleType())) \
.withColumn("Withdrawls",regexp_replace(df.Withdrawls,",","").cast(DoubleType())) \
.withColumn("Balance",regexp_replace(df.Balance,",","").cast(DoubleType()))

df = df.join(static_df, df.Description==static_df.type)

df.writeStream \
  .outputMode("update") \
  .format("console") \
  .option("truncate", "false") \
  .start() \
  .awaitTermination()