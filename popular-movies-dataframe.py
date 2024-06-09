from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# create schema
schema = StructType([
                        StructField("userID", IntegerType(), True),
                        StructField("movieID", IntegerType(), True),
                        StructField("rating", IntegerType(), True),
                        StructField("timestamp", LongType(), True),
                    ])

#  load up movie data as a dataframe
movies = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

# some sql style magic to sort all the movies by popularity in one line
topMovieIDs = movies.groupBy("movieID").count().orderBy(func.desc("count"))

# grab the top 10
topMovieIDs.show(10)

# stop the session
spark.stop()
