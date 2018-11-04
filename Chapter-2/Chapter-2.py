# Code for Chapter 2


# Example 2-1
lines = sc.textFile("README.md") # Create an RDD called lines
lines.count() # Count the number of items in this RDD
lines.first() # First item in this RDD, i.e. first line of README.md



# Example 2-4
lines = sc.textFile("README.md")
pythonLines = lines.filter(lambda line: "Python" in line)
pythonLines.first()


# Example 2-7
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf) 
