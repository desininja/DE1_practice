from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession \
    .builder \
    .appName("analyze transactions") \
    .config("spark.sql.shuffle.partitions", 2) \
    .enableHiveSupport().getOrCreate()

spark.table("credit_card.filtered_transactions") \
    .groupBy("item_id") \
    .count() \
    .write \
    .save("hdfs:///data/credit_card/transactions_per_item", format='json', mode='overwrite')
