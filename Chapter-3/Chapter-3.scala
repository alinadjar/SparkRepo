//Code for Chapter 3

// Example 3-2
val lines = sc.parallelize(List("pandas", "i like pandas"))



// Example 3-4
val inputRDD = sc.textFile("log.txt")
val errorsRDD = inputRDD.filter(line => line.contains("error"))


// Example 3-7
println("Input had " + badLinesRDD.count() + " concerning lines")
println("Here are 10 examples:")
badLinesRDD.take(10).foreach(println)



// Example 3-9
val input = sc.parallelize(List(1, 2, 3, 4))
val result = input.map(x => x * x)
println(result.collect().mkString(","))



// Example 3-11
val sum = rdd.reduce((x, y) => x + y)


// Example 3-13
val result = input.aggregate((0, 0))(
                               (acc, value) => (acc._1 + value, acc._2 + 1),
                               (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
val avg = result._1 / result._2.toDouble



// Example 3-17
class SearchFunctions(val query: String) {
     def isMatch(s: String): Boolean = {
          s.contains(query)
     }
     def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
          // Problem: "isMatch" means "this.isMatch", so we pass all of "this"
          rdd.map(isMatch)
     }
     def getMatchesFieldReference(rdd: RDD[String]): RDD[String] = {
          // Problem: "query" means "this.query", so we pass all of "this"
          rdd.map(x => x.split(query))
     }
     def getMatchesNoReference(rdd: RDD[String]): RDD[String] = {
          // Safe: extract just the field we need into a local variable
          val query_ = this.query
          rdd.map(x => x.split(query_))
     }
}



// Example 3-18
val result = input.map(x => x * x)
result.persist(StorageLevel.DISK_ONLY)
println(result.count())
println(result.collect().mkString(","))


