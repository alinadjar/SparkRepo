//Code for Chapter 2
// This file contains Scala code to be executed in Spark shell only.


// Example 2-2
val lines = sc.textFile("README.md") // Create an RDD called lines
lines.count() // Count the number of items in this RDD
lines.first() // First item in this RDD, i.e. first line of README.md



// Example 2-5
val lines = sc.textFile("README.md") // Create an RDD called lines
val pythonLines = lines.filter(line => line.contains("Python"))
pythonLines.first()



// Example 2-8
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
val conf = new SparkConf().setMaster("local").setAppName("My App")
val sc = new SparkContext(conf) 




// Example 2-11
// Create a Scala Spark Context.
val conf = new SparkConf().setAppName("wordCount")
val sc = new SparkContext(conf)
// Load our input data.
val input = sc.textFile(inputFile)
// Split it up into words.
val words = input.flatMap(line => line.split(" "))
// Transform into pairs and count.
val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
// Save the word count back out to a text file, causing evaluation.
counts.saveAsTextFile(outputFile)
