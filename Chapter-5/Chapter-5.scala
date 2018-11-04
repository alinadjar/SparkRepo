//Code for Chapter 5
// This file contains Scala code to be executed in Spark shell only.


// Example 5-1
val input = sc.textFile("file:///home/holden/repos/spark/README.md")


// Example 5-3
val input = sc.wholeTextFiles("file://home/holden/salesFiles")
val result = input.mapValues{y =>
    val nums = y.split(" ").map(x => x.toDouble)
    nums.sum / nums.size.toDouble
}



// Example 5-6
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
...
case class Person(name: String, lovesPandas: Boolean) // Must be a top-level class
...
// Parse it into a specific case class. We use flatMap to handle errors
// by returning an empty list (None) if we encounter an issue and a
// list with one element if everything is ok (Some(_)).
val result = input.flatMap(record => {
   try {
    Some(mapper.readValue(record, classOf[Person]))
} catch {
    case e: Exception => None
}})




// Example 5-8
result.filter(p => P.lovesPandas).map(mapper.writeValueAsString(_))
      .saveAsTextFile(outputFile)



// Example 5-9
import Java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
...
val input = sc.textFile(inputFile)
val result = input.map{ line =>
   val reader = new CSVReader(new StringReader(line));
    reader.readNext();
}




// Example 5-11
case class Person(name: String, favoriteAnimal: String)
val input = sc.wholeTextFiles(inputFile)
val result = input.flatMap{ case (_, txt) =>
    val reader = new CSVReader(new StringReader(txt));
    reader.readAll().map(x => Person(x(0), x(1)))
}




// Example 5-13
pandaLovers.map(person => List(person.name, person.favoriteAnimal).toArray)
.mapPartitions{people =>
   val stringWriter = new StringWriter();
   val csvWriter = new CSVWriter(stringWriter);
   csvWriter.writeAll(people.toList)
   Iterator(stringWriter.toString)
}.saveAsTextFile(outFile)



// Example 5-15
val data = sc.sequenceFile(inFile, classOf[Text], classOf[IntWritable]).
       map{case (x, y) => (x.toString, y.get())}



// Example 5-17
val data = sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2)))
data.saveAsSequenceFile(outputFile)



// Example 5-18
val input = sc.hadoopFile[Text, Text, KeyValueTextInputFormat](inputFile).map{
      case (x, y) => (x.toString, y.toString)
}



// Example 5-19
val input = sc.newAPIHadoopFile(inputFile, classOf[LzoJsonInputFormat],
     classOf[LongWritable], classOf[MapWritable], conf)
// Each MapWritable in "input" represents a JSON object




// Example 5-21
val job = new Job()
val conf = job.getConfiguration
LzoProtobufBlockOutputFormat.setClassConf(classOf[Places.Venue], conf);
val dnaLounge = Places.Venue.newBuilder()
dnaLounge.setId(1);
dnaLounge.setName("DNA Lounge")
dnaLounge.setType(Places.Venue.VenueType.CLUB)
val data = sc.parallelize(List(dnaLounge.build()))
val outputData = data.map{ pb =>
     val protoWritable = ProtobufWritable.newInstance(classOf[Places.Venue]);
     protoWritable.set(pb)
     (null, protoWritable)
}
outputData.saveAsNewAPIHadoopFile(outputFile, classOf[Text],
    classOf[ProtobufWritable[Places.Venue]],
    classOf[LzoProtobufBlockOutputFormat[ProtobufWritable[Places.Venue]]], conf)



// Example 5-22
val  rdd = sc.textFile("file:///home/holden/happypandas.gz")


// Example 5-23
import org.apache.spark.sql.hive.HiveContext
val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)
val rows = hiveCtx.sql("SELECT name, age FROM users")
val firstRow = rows.first()
println(firstRow.getString(0)) // Field 0 is the name



// Example 5-26
val tweets = hiveCtx.jsonFile("tweets.json")
tweets.registerTempTable("tweets")
val results = hiveCtx.sql("SELECT user.name, text FROM tweets")
