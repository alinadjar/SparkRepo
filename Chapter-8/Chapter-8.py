# Code for Chapter 8


# Example 8-1
# Construct a conf
conf = new SparkConf()
conf.set("spark.app.name", "My Spark App")
conf.set("spark.master", "local[4]")
conf.set("spark.ui.port", "36000") # Override the default port
# Create a SparkContext with this configuration
sc = SparkContext(conf)




# Example 8-3
$ bin/spark-submit \
   --class com.example.MyApp \
   --master local[4] \
   --name "My Spark App" \
   --conf spark.ui.port=36000 \
   myApp.jar

    
    

# Example 8-4
$ bin/spark-submit \
   --class com.example.MyApp \
   --properties-file my-config.conf \
   myApp.jar

## Contents of my-config.conf ##
spark.master local[4]
spark.app.name "My Spark App"
spark.ui.port 36000


# Example 8-10
# Wildcard input that may match thousands of files
input = sc.textFile("s3n://log-files/2014/*.log")
input.getNumPartitions()

# A filter that excludes almost all data
lines = input.filter(lambda line: line.startswith("2014-10-17"))
lines.getNumPartitions()
# We coalesce the lines RDD before caching
lines = lines.coalesce(5).cache()
lines.getNumPartitions()
# Subsequent analysis can operate on the coalesced RDD...
lines.count()




