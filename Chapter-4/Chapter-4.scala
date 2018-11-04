//Code for Chapter 4



// ٍExample 4-2
val pairs = lines.map(x => (x.split(" ")(0), x))

// Example 4-4
pairs.filter{case (key, value) => value.length < 20}

// Example 4-6
rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

// Example 4-8
val input = sc.textFile("s3://...")
val words = input.flatMap(x => x.split(" "))
val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)


// Example 4-10
val result = input.combineByKey(
                   (v) => (v, 1),     
                   (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),    
                   (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)     
           ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }   
    result.collectAsMap().map(println(_)) 


// Example 4-12
val data = Seq(("a", 3), ("b", 4), ("a", 1))
sc.parallelize(data).reduceByKey((x, y) => x + y) // Default parallelism
sc.parallelize(data).reduceByKey((x, y) => x + y) // Custom parallelism



// Example 4-14
storeAddress = {
   (Store("Ritual"), "1026 Valencia St"), (Store("Philz"), "748 Van Ness Ave"),
   (Store("Philz"), "3101 24th St"), (Store("Starbucks"), "Seattle")}
storeRating = {
   (Store("Ritual"), 4.9), (Store("Philz"), 4.8))}
storeAddress.join(storeRating) == {
   (Store("Ritual"), ("1026 Valencia St", 4.9)),
   (Store("Philz"), ("748 Van Ness Ave", 4.8)),
   (Store("Philz"), ("3101 24th St", 4.8))}


// Example 4-15
storeAddress.leftOuterJoin(storeRating) ==
{(Store("Ritual"),("1026 Valencia St",Some(4.9))),
  (Store("Starbucks"),("Seattle",None)),
  (Store("Philz"),("748 Van Ness Ave",Some(4.8))),    
  (Store("Philz"),("3101 24th St",Some(4.8)))}
storeAddress.rightOuterJoin(storeRating) ==
{(Store("Ritual"),(Some("1026 Valencia St"),4.9)),
  (Store("Philz"),(Some("748 Van Ness Ave"),4.8)),
(Store("Philz"), (Some("3101 24th St"),4.8))}  



// Example 4-17
val input: RDD[(Int, Venue)] = ...
implicit val sortIntegersByString = new Ordering[Int] {
    override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
}
rdd.sortByKey() 




// Example 4-18
// Initialization code; we load the user info from a Hadoop SequenceFile on HDFS.
// This distributes elements of userData by the HDFS block where they are found,
// and doesn't provide Spark with any way of knowing in which partition a
// particular UserID is located.
val sc = new SparkContext(...)
val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...").persist()

// Function called periodically to process a logfile of events in the past 5 minutes;
// we assume that this is a SequenceFile containing (UserID, LinkInfo) pairs.
def processNewLogs(logFileName: String) {
    val events = sc.sequenceFile[UserID, LinkInfo](logFileName)
    val joined = userData.join(events)// RDD of (UserID, (UserInfo, LinkInfo)) pairs  
    val offTopicVisits = joined.filter {  
        case (userId, (userInfo, linkInfo)) => // Expand the tuple into its components
           !userInfo.topics.contains(linkInfo.topic)    
}.count()    
    println("Number of visits to non-subscribed topics: " + offTopicVisits)       
}



// Example 4-19
val sc = new SparkContext(...)
val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...")
                            .partitionBy(new HashPartitioner(100)) // Create 100 partitions
                            .persist()



// Example 4-20
val pairs = sc.parallelize(List((1, 1), (2, 2), (3, 3)))
pairs.partitioner
val partitioned = pairs.partitionBy(new spark.HashPartitioner(2))
partitioned.partitioner




// Example 4-21
// Assume that our neighbor list was saved as a Spark objectFile
val links = sc.objectFile[(String, Seq[String])]("links")
                      .partitionBy(new HashPartitioner(100))
                      .persist()

// Initialize each page's rank to 1.0; since we use mapValues, the resulting RDD
// will have the same partitioner as links
var ranks = links.mapValues(v => 1.0)

// Run 10 iterations of PageRank
for (i <- 0 until 10) {
    val contributions = links.join(ranks).flatMap {   
        case (pageId, (links, rank)) =>
           links.map(dest => (dest, rank / links.size))
     }
     ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
}

// Write out the final ranks
ranks.saveAsTextFile("ranks")




// Example 4-22
class DomainNamePartitioner(numParts: Int) extends Partitioner {
   override def numPartitions: Int = numParts
   override def getPartition(key: Any): Int = {
       val domain = new Java.net.URL(key.toString).getHost()
       val code = (domain.hashCode % numPartitions)
       if (code < 0) {
         code + numPartitions // Make it non-negative
      } else {
           code
      }
}
// Java equals method to let Spark compare our Partitioner objects
   override def equals(other: Any): Boolean = other match {
     case dnp: DomainNamePartitioner =>
         dnp.numPartitions == numPartitions
     case _ =>
        false
   }
}


