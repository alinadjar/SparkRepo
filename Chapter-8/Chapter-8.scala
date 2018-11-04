// Code for Chapter 8


// Example 8-2
// Construct a conf
val conf = new SparkConf()
conf.set("spark.app.name", "My Spark App")
conf.set("spark.master", "local[4]")
conf.set("spark.ui.port", "36000") // Override the default port
// Create a SparkContext with this configuration
val sc = new SparkContext(conf)



// Example 8-3
$ bin/spark-submit \
   --class com.example.MyApp \
   --master local[4] \
   --name "My Spark App" \
   --conf spark.ui.port=36000 \
   myApp.jar





// Example 8-6
val input = sc.textFile("input.txt")
val tokenized = input.map(line => line.split(" ")).filter(words => words.size > 0)
val counts = tokenized.map(words => (words(0), 1)).reduceByKey{ (a, b) => a + b }


// Example 8-7
input.toDebugString
counts.toDebugString



// Example 8-8
counts.collect()




// Example 8-9
counts.cache()
counts.collect()
counts.collect()




// Example 8-11
val conf = new SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
// Be strict about class registration
conf.set("spark.kryo.registrationRequired", "true")
conf.registerKryoClasses(Array(classOf[MyClass], classOf[MyOtherClass]))
