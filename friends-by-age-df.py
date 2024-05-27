from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round

spark = SparkSession.builder.appName("friends_df").getOrCreate()

friends_df = spark.read.csv("file:///SparkCourse/fakefriends-header.csv", header=True, inferSchema=True)

friends_df.select("age", "friends").groupBy("age").agg(round(avg("friends"), 2).alias("avg_friends")).sort("age").show()
