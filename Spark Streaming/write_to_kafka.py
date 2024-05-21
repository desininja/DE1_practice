from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Initialize Spark Session
spark = SparkSession  \
    .builder  \
    .appName("StructuredSocketRead")  \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Define the schema for the CSV data
mySchema = StructType().add("name", "string").add("age", "integer")

# Read from the CSV directory
lines = spark  \
    .readStream  \
    .option("delimiter", ";")  \
    .format("csv")  \
    .schema(mySchema)  \
    .load("/Users/himanshu/Desktop/Upgrad Revision/DE1_practice/Spark Streaming/csv_input_dir/")  # Directory containing the CSV files

# Select and cast the required columns
kafkaDF = lines.selectExpr("name as key", "cast(age as string) as value")

# Debug: Print schema of the DataFrame to verify
kafkaDF.printSchema()

# Write to Kafka
query = kafkaDF  \
    .writeStream  \
    .outputMode("append")  \
    .format("kafka")  \
    .option("kafka.bootstrap.servers", "localhost:9092")  \
    .option("topic", "test")  \
    .option("checkpointLocation", "/Users/himanshu/Desktop/Upgrad Revision/DE1_practice/Spark Streaming/checkpoint_dir") \
    .start()
print(kafkaDF)
# Await termination
query.awaitTermination()
