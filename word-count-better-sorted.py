import re
from pyspark import SparkConf, SparkContext

def normalize_words(text):
    """
    Uses regex to trim down to clean words
    Args:
        text (rdd): text from input file

    Returns:
        list: lower case words culled from input text
    """
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster('local').setAppName('WordCount')
sc = SparkContext(conf = conf)

input = sc.textFile("file:///SparkCourse/Book")
words = input.flatMap(normalize_words)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word.decode() + ":\t\t" + count)
