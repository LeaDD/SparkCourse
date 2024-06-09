from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("totalSpent").getOrCreate()

schema = StructType([
                        StructField("cust_id", IntegerType(), True),
                        StructField("order_id", StringType(), True),
                        StructField("amt_spent", FloatType(), True)
                    ])

# read in the file
input = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")

# aggregate by cust_id, sum and sort
spent_by_customer = input.select("cust_id", "amt_spent").groupBy("cust_id").agg(func.sum("amt_spent").alias("total_spent")).sort(func.desc("total_spent"))
spent_by_customer.printSchema()

results = spent_by_customer.collect()

# show results
for result in results:
    print(str(result[0]) + f"\t{result[1]:.2f}")
