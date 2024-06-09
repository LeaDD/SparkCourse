from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

def loadMovieNames():
    movieNames = {}
    with open(r"C:\SparkCourse\ml-100k\u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([
                StructField("userID", IntegerType(), True),
                StructField("movieID", IntegerType(), True),
                StructField("rating", IntegerType(), True),
                StructField("timestamp", LongType(), True)
            ])

# load movie data as dataframe
movies = spark.read.option('sep', '\t').schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

movie_counts = movies.groupBy('movieID').count()

# create user defined function to lookup movie names from our broadcasted dictionary
def lookup_name(movie):
    return nameDict.value[movie]

lookupNameUDF = func.udf(lookup_name)

# add a movie title column using our new udf
movies_with_names = movie_counts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# sort the results
sorted_movies_with_names = movies_with_names.orderBy(func.desc("count"))

# show the top 10
sorted_movies_with_names.show(10)

spark.stop()
