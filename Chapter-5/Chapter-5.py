# Code for Chapter 5


# Example 5-2
input = sc.textFile("file:///home/holden/repos/spark/README.md")


# Example 5-4
# saveAsTextFile() takes a path to a folder as parameter, not a path to a file. It will actually write one file per partition in that folder, named part-r-xxxxx (xxxxx being 00000 to whatever number of partitions you have).
result.saveAsTextFile(outputFile)




# Example 5-5, 5-7

from pyspark import SparkContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Error usage: LoadJson [sparkmaster] [inputfile] [outputfile]"
        sys.exit(-1)
    master = sys.argv[1]
    inputFile = sys.argv[2]
    outputFile = sys.argv[3]
    sc = SparkContext(master, "LoadJson")
    input = sc.textFile(inputFile)
    data = input.map(lambda x: json.loads(x))
    data.filter(lambda x: 'lovesPandas' in x and x['lovesPandas']).map(
        lambda x: json.dumps(x)).saveAsTextFile(outputFile)
    sc.stop()
    print "Done!"

    
    
    
    
# load json using DataFrame API    
from pyspark.sql import SparkSession
df=spark.read.json("pandainfo.json")
df.show()










# Example 5-10, 12, 14


from pyspark import SparkContext
import csv
import sys
import StringIO


def loadRecord(line):
    """Parse a CSV line"""
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
    return reader.next()


def loadRecords(fileNameContents):
    """Load all the records in a given file"""
    input = StringIO.StringIO(fileNameContents[1])
    reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
    return reader


def writeRecords(records):
    """Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "favouriteAnimal"])
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Error usage: LoadCsv [sparkmaster] [inputfile] [outputfile]"
        sys.exit(-1)
    master = sys.argv[1]
    inputFile = sys.argv[2]
    outputFile = sys.argv[3]
    sc = SparkContext(master, "LoadCsv")
    # Try the record-per-line-input
    input = sc.textFile(inputFile)
    data = input.map(loadRecord)
    pandaLovers = data.filter(lambda x: x['favouriteAnimal'] == "panda")
    pandaLovers.mapPartitions(writeRecords).saveAsTextFile(outputFile)
    # Try the more whole file input
    fullFileData = sc.wholeTextFiles(inputFile).flatMap(loadRecords)
    fullFilePandaLovers = fullFileData.filter(
        lambda x: x['favouriteAnimal'] == "panda")
    fullFilePandaLovers.mapPartitions(
        writeRecords).saveAsTextFile(outputFile + "fullfile")
    sc.stop()
    print "Done!"






# Example 5-16
data = sc.sequenceFile(inFile,
    "org.apache.hadoop.io.Text", "org.apache.hadoop.io.IntWritable")



# Example 5-24
from pyspark.sql import HiveContext
hiveCtx = HiveContext(sc)
rows = hiveCtx.sql("SELECT name, age FROM users")
firstRow = rows.first()
print firstRow.name




# Example 5-27
from pyspark.sql import HiveContext
hiveCtx = HiveContext(sc)
tweets = hiveCtx.read.json("testweet.json")
tweets.registerTempTable("tweets")
results = hiveCtx.sql("SELECT user.name, text FROM tweets")
results.show()


# Example 5-28
# Load some data in from a Parquet file with field's name and favouriteAnimal
rows = hiveCtx.parquetFile(parquetFile)
names = rows.map(lambda row: row.name)
print "Everyone"
print names.collect()




# Example 5-29
# Find the panda lovers
tbl = rows.registerTempTable("people")
pandaFriends = hiveCtx.sql("SELECT name FROM people WHERE favouriteAnimal = \"panda\"")
print "Panda friends"
print pandaFriends.map(lambda row: row.name).collect()




# Example 5-30
pandaFriends.saveAsParquetFile("hdfs://...")
