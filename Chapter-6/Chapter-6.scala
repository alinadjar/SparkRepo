//Code for Chapter 6
// This file contains Scala code to be executed in Spark shell only.


// Example 6-1
val accum = sc.longAccumulator(“My Acc”)
sc.parallelize(Array(1,2,3,4)).foreach(x => accum.add(x))




// Example 6-3
val sc = new SparkContext(...)
val file = sc.textFile("file.txt")

val blankLines = sc.accumulator(0) // Create an Accumulator[Int] initialized to 0

val callSigns = file.flatMap(line => {
    if (line == "") {
        blankLines += 1 // Add to the accumulator
    }
    line.split(" ")
 })

callSigns.saveAsTextFile("output.txt")
println("Blank lines: " + blankLines.value)




// Example 6-6
val  accum = sc.longAccumulator
data.map{x => accum.add(x); x}



// Example 6-7
class VectorAccumulatorV2 extends AccumulatorV2[MyVector, MyVector] {

  private val myVector: MyVector = MyVector.createZeroVector

  def reset(): Unit = {
    myVector.reset()
  }

  def add(v: MyVector): Unit = {
    myVector.add(v)
  }
  ...
}

// Then, create an Accumulator of this type:
val myVectorAcc = new VectorAccumulatorV2
// Then, register it into spark context:
sc.register(myVectorAcc, "MyVectorAcc1")




// Example 6-8
object VectorAccumulatorParam extends AccumulatorParam[Array[Double]] {

     def addInPlace(t1: Array[Double], t2: Array[Double]): Array[Double] = {

         require(t1.length == t2.length)

         (0 until t1.length).foreach{idx =>

            t1(idx) += t2(idx)

          }

          t1

    }

}



val nBuckets = ... // maybe 10K

val bucketCounts = sc.accumulator(new Array[Double](nBuckets))(VectorAccumulatorParam)

val myData: RDD[User] = ...




myData.map { user => 

val bucketIdx = assignUserToBucket(user)

bucketCounts.localValue(bucketIdx) += 1

} ...





// Example 6-9
// if you forget to give your accumulator a name here, it won't show up in the UI

val parseErrors = sc.accumlator(0L, "parse errors")

val parsedData: RDD[Users] = sc.textFile(pathToMyData).flatMap { line =>

  try {

     Some(parseUser(line))

    } catch {

       case pe: ParseException =>

       parseErrrors += 1

       None

      }

}



// now what? can't look at parseErrors, since the RDD transformation above is lazy



// ok lets say we wait till we execute some action...

parsedData.cache()

parsedData.count()

// now we could look at the value

if (parseErrors.value > 50) throw new RuntimeException("too many bad records, aborting!")



doSomeOtherStuff()



// but what if someone comes along and sticks in another action referencing the same RDD?

parsedData.filter { user => user.lastActive.after("2015-01-01") }.count()

// and we move our check down here? it should be fine, right, since we cached parsedData?

// oops ... what if do Some Other Stuff() cached another RDD which kicked parsed Data out of

// the cache (or maybe only some partitions are kicked out)? So this could easily get

// triggered even if there were only 25 records with parse errors

if (parseErrors.value > 50) throw new RuntimeException("too many bad records, aborting!")




// Example 6-10
val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar.value




// Example 6-11
val hoods = Seq((1, "Mission"), (2, "SOMA"), (3, "Sunset"), (4, "Haight Ashbury"))
val checkins = Seq((234, 1),(567, 2), (234, 3), (532, 2), (234, 4))
val hoodsRdd = sc.parallelize(hoods)
val checkRdd = sc.parallelize(checkins)

val broadcastedHoods = sc.broadcast(hoodsRdd.collectAsMap())
val checkinsWithHoods = checkRdd.mapPartitions({row =>
 row.map(x => (x._1, x._2, broadcastedHoods.value.getOrElse(x._2, -1)))
}, preservesPartitioning = true)

checkinsWithHoods.take(5)




// Example 6-14
val rdd1 =  sc.parallelize(List("yellow","red","blue","cyan","black"),3)         
val mapped =rdd1.mapPartitionsWithIndex{
     // 'index' represents the Partition No
     // 'iterator' to iterate through all elements
     //   in the partition
 (index, iterator) => {
                         println("Called in Partition -> " + index)
                         val myList = iterator.toList
     // In a normal user case, we will do the
     // the initialization(ex : initializing database)
     // before iterating through each element
                         myList.map(x => x + " -> " + index).iterator
                       }
   }
      mapped.collect()






// Example 6-16
val data = List("hi","hello","how","are","you")
val dataRDD = sc.makeRDD(data) //sc is SparkContext
val scriptPath = "/home/hadoop/echo.sh"
val pipeRDD = dataRDD.pipe(scriptPath)
pipeRDD.collect()






// Example 6-18
// Now we can go ahead and remove outliers since those may have misreported locations
// first we need to take our RDD of strings and turn it into doubles.
val distanceDouble = distance.map(string => string.toDouble)
val stats = distanceDoubles.stats()
val stddev = stats.stdev
val mean = stats.mean
val reasonableDistances = distanceDoubles.filter(x => math.abs(x-mean) < 3 * stddev)
println(reasonableDistance.collect().toList)



