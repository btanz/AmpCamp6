"""
    First day exercises of AMPCAMP 6:
        1. Exploring the Wikipedia dataset

    Created by Benjamin Tanz
    For datafiles, see
    http://ampcamp.berkeley.edu/6/exercises/getting-started.html
"""

from pyspark import SparkContext

SEPARATOR = "*******"

# Set the spark context
sc = SparkContext("local", "WikiQuery")

# Read textfile into RDD
pagecounts = sc.textFile("../data/pagecounts")

# Print the first k records
# This returns an array with comma-separated values
print(SEPARATOR)
print(pagecounts.take(10))

# Print a more readable output by traversing each entry
print(SEPARATOR)
for entry in pagecounts.take(10):
    print(entry)

# Count the number of records in the dataset
print(SEPARATOR)
print(pagecounts.count())

# Find only english entries
# Cache the RDD for quick future access; the filtered RDD will then be kept in memory after the first evaluation
enPages = pagecounts.filter(lambda x: x.split(" ")[1] == "en").cache()

# Find the number of english language pages
print(SEPARATOR)
print(enPages.count())

# Create a histogram of total page views for english language pages by date
print(SEPARATOR)
enTuples = enPages.map(lambda x: x.split(" "))  # create array of arrays with lines and space-separable strings per line
enKeyValuePairs = enTuples.map(lambda x: (x[0][:8], int(x[3])))  # create key-value pairs for dates and pagecounts
enCounts = enKeyValuePairs.reduceByKey(lambda a, b: a + b, 1).collect()   # group all vals of same key together
print(enCounts)

# Find english pages with more than 200000 pageviews during the 3 days of data coverage
print(SEPARATOR)
enCountsBig =enPages.map(lambda x: x.split(" ")).map(lambda x: (x[2], int(x[3]))).reduceByKey(lambda x, y: x + y, 40).filter(lambda x: x[1] > 200000).collect()
print(enCountsBig)