"""
    First day exercises of AMPCAMP 6:
        2. Data exploration using SparkSQL

    Created by Benjamin Tanz
    For datafiles, see
    http://ampcamp.berkeley.edu/6/exercises/getting-started.html
"""

# import spark
from pyspark import SparkContext
from pyspark.sql import SQLContext

# separator token for screen output
SEPARATOR = "*******"


# Initialize the spark and sql contexts
sc = SparkContext("local", "WikiQuerySQL")
sqlContext = SQLContext(sc)

# load data from disc; results in the SchemaRDD wikiData
wikiData = sqlContext.read.parquet("../data/wiki.parquet")

# retrieve number of records
print(SEPARATOR)
print(wikiData.count())

# register data as a table such that we can run SQL queries and run a query
# result is a collection of row objects
print(SEPARATOR)
wikiData.registerTempTable("wikiData")
result = sqlContext.sql("SELECT COUNT(*) AS pageCount FROM wikiData").collect()
print(result[0].pageCount)


# return the top ten user names by the number of pages they created
resultUser = sqlContext.sql("SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10").collect()
print(resultUser)
