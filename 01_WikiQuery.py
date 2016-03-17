""" Day 1 exercise of AMPCAMP 6: Exploring the Wikipedia dataset
    Created by Benjamin Tanz
    For datafiles, see
    http://ampcamp.berkeley.edu/6/exercises/getting-started.html
"""

from pyspark import SparkContext

# Set the spark context
sc = SparkContext("local", "WikiQuery")

# Read textfile into RDD
pagecounts = sc.textFile("../data/pagecounts")

# Print the first k records
# This returns an array with comma-separated values
print(pagecounts.take(10))

# Print a more readable output by traversing each entry
print("*****")
for entry in pagecounts.take(10):
    print(entry)

#