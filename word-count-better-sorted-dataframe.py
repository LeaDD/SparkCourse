from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# read each line of the book into a dataframe
input_DF = spark.read.text("file:///SparkCourse/Book")

# split using a regular expression that extracts words
words = input_DF.select(func.explode(func.split(input_DF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# normalize everything in lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# sort by counts
wordCountsSorted = wordCounts.sort("count")

# show the results
wordCountsSorted.show(wordCountsSorted.count())
