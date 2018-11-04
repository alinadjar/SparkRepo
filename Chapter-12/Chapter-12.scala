//Code for Chapter 12


// Example 12-1
import org.apache.spark.{SparkConf, SparkContext}

object grafX_1 {
  def main(args: Array[String]): Unit =
  {
    import org.apache.spark.graphx._

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sheet")

    val sc = new SparkContext(conf)

    val myVertices = sc.parallelize(Array(
      (1L, "Fawad"),
      (2L, "Aznan"),
      (3L, "Ben"),
      (4L, "Tom"),
      (5L, "Marathon"))
    )
    val myEdges = sc.parallelize(Array(
      Edge(1L, 5L, "Runs-A"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(1L, 2L, "Likes-status"),
      Edge(2L, 4L,"Is-Friends-With")))

    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect()

    val r1 = myGraph.vertices.count()
    print("Number of Vertices  = " + r1 + "\n")

    myGraph.edges.foreach(println)
      
      
    //Find all Vertices where the name property in Vertice="Fawad"
    myGraph.vertices.filter{ case (id,name)=> name=="Fawad" }.count()

  }

}

===============================  output:
Number of Vertices  = 5
Edge(1,2,Likes-status)
Edge(1,5,Runs-A)
Edge(2,3,is-friends-with)
Edge(2,4,Is-Friends-With)
Edge(3,4,is-friends-with)






val myGraph = GraphLoader.edgeListFile(sc, “/home/spark/sampledata/callrecords.txt”)
myGraph.vertices.count()



// Example 12-2
import org.apache.spark.graphx._

case class User(name: String, age: Int)

val users = List((1L, User("Alex", 26)), (2L, User("Bill", 42)), (3L, User("Carol", 18)),
                        (4L, User("Dave", 16)), (5L, User("Eve", 45)), (6L, User("Farell", 30)),
                        (7L, User("Garry", 32)), (8L, User("Harry", 36)), (9L, User("Ivan", 28)),
                        (10L, User("Jill", 48))
                      )
val usersRDD = sc.parallelize(users)

val follows = List(Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 1L, 1), Edge(3L, 4L, 1),
                            Edge(3L, 5L, 1), Edge(4L, 5L, 1), Edge(6L, 5L, 1), Edge(7L, 6L, 1),
                            Edge(6L, 8L, 1), Edge(7L, 8L, 1), Edge(7L, 9L, 1), Edge(9L, 8L, 1),
                            Edge(8L, 10L, 1), Edge(10L, 9L, 1), Edge(1L, 11L, 1)
                        )
val followsRDD = sc.parallelize(follows)
val defaultUser = User("NA", 0)
val socialGraph = Graph(usersRDD, followsRDD, defaultUser)





// mask API
val femaleConnections = List(Edge(2L, 3L, 0), Edge(3L, 1L, 0), Edge(3L, 4L, 0),
                                                   Edge(3L, 5L, 0), Edge(4L, 5L, 0), Edge(6L, 5L, 0),
                                                   Edge(8L, 10L, 0), Edge(10L, 9L, 0)
                                              )
val femaleConnectionsRDD = sc.parallelize(femaleConnections)
val femaleGraphMask = Graph.fromEdges(femaleConnectionsRDD, defaultUser)
val femaleGraph = socialGraph.mask(femaleGraphMask)
femaleGraphMask.triplets.take(10)




// groupEdges API
val multiEdges = List(Edge(1L, 2L, 100), Edge(1L, 2L, 200),
                                      Edge(2L, 3L, 300), Edge(2L, 3L, 400),
                                      Edge(3L, 1L, 200), Edge(3L, 1L, 300)
                                  )
val multiEdgesRDD = sc.parallelize(multiEdges)
val defaultVertexProperty = 1
val multiEdgeGraph = Graph.fromEdges(multiEdgesRDD, defaultVertexProperty)
import org.apache.spark.graphx.PartitionStrategy._
val repartitionedGraph = multiEdgeGraph.partitionBy(CanonicalRandomVertexCut)
val singleEdgeGraph = repartitionedGraph.groupEdges((edge1, edge2) => edge1 + edge2)
multiEdgeGraph.edges.collect()





// outerJoinVertices
case class UserWithCity(name: String, age: Int, city: String)
val userCities = sc.parallelize(List((1L, "Boston"), (3L, "New York"), (5L, "London"),
                                                           (7L, "Bombay"), (9L, "Tokyo"), (10L, "Palo Alto")))
val socialGraphWithCity = socialGraph.outerJoinVertices(userCities)((id, user, cityOpt) =>
                                                        cityOpt match {       case Some(city) => UserWithCity(user.name, user.age, city)
                                                                                          case None => UserWithCity(user.name, user.age, "NA")
                                                                                 })
socialGraphWithCity.vertices.take(5)






// Pregel
val outDegrees = socialGraph.outDegrees()
val outDegreesGraph = socialGraph.outerJoinVertices(outDegrees) {  (vId, vData, OptOutDegree) =>    
    OptOutDegree.getOrElse(0)
                                                                                                                    }
val weightedEdgesGraph = outDegreesGraph.mapTriplets{EdgeTriplet =>
    
    
    1.0 / EdgeTriplet.srcAttr                                                                                                    }
val inputGraph = weightedEdgesGraph.mapVertices((id, vData) => 1.0)
val firstMessage = 0.0
val iterations = 20
val edgeDirection = EdgeDirection.Out
val updateVertex = (vId: Long, vData: Double, msgSum: Double) => 0.15 + 0.85 * msgSum
val sendMsg = (triplet: EdgeTriplet[Double, Double]) => Iterator((triplet.dstId, triplet.srcAttr * triplet.attr))
val aggregateMsgs = (x: Double, y: Double ) => x + y
val influenceGraph = inputGraph.pregel(firstMessage, iterations, edgeDirection)(updateVertex,
                                                                      sendMsg, aggregateMsgs)
val userNames = socialGraph.mapVertices{(vId, vData) => vData.name}.vertices()
val userNamesAndRanks = influenceGraph.outerJoinVertices(userNames) { (vId, rank, optUserName) =>    
    (optUserName.get, rank)
}.vertices()
userNamesAndRanks.collect.foreach{ case(vId, vData)  =>  println(vData._1 +"'s influence rank: " + vData._2)  }





// PageRank
import org.apache.spark.graphx.{Graph, VertexRDD, GraphLoader}
val cdrGraph = GraphLoader.edgeListFile(sc,"/home/spark/sampledata/graphx/cdrs.txt")
val influencers = cdrGraph.pageRank(0.0001).vertices()
val usersList = sc.textFile("/home/spark/sampledata/graphx/usernames.csv").map{line =>
                                               val fields = line.split(",")
                                               (fields(0).trim().toLong, fields(1))
                                          }
val ranksByUsername = usersList.join(influencers).map {
                                                                     case (id, (username, userRank)) => (username, userRank)
                                                                   }
println(ranksByUsername.collect().mkString("\n"))





// Connected Components
import org.apache.spark.graphx._
val cdrGraph = GraphLoader.edgeListFile(sc,"/home/spark/sampledata/graphx/cdrs.txt")
val connectedVertices = cdrGraph.connectedComponents().vertices
val usersList = sc.textFile("/home/spark/sampledata/graphx/usernames.csv").map{line =>
                                                val fields = line.split(",")
                                                 (fields(0).trim().toLong, fields(1))
                                              }
val connectedComponentsByUsers = usersList.join(connectedVertices).map {
                                                  case (id, (username, cc)) => (username, cc)
                                                }
println(connectedComponentsByUsers.collect().mkString("\n"))






// GraphFrames
import org.graphframes.GraphFrame
//Creating a Vertices Data Frame - Remember to specify the "id" column
val vertices = spark.createDataFrame(List(
                             ("Maggie","UK",28,"Female"),("Jennifer","US",42,"Female"),
                              ("Roger","US",42,"Male"),("Ben","US",30,"Male"),
                              ("Tom","UK",41,"Male"),("Terrorism","N/A",0,"N/A"),
                              ("Hate-Speech","N/A",0,"N/A"),("Sports","N/A",0,"N/A"),
                              ("Politics","N/A",0,"N/A"))).toDF("id","Country","Age","Gender")
//Creating an Edges Data Frame - Remember to specify the "src" and "dst"
//columns
val edges = spark.createDataFrame(List(
                              ("Maggie","Terrorism","Creates"),("Ben","Terrorism","Likes"),
                              ("Maggie","Hate-Speech","Creates"),("Jennifer","Terrorism","Likes"),
                              ("Maggie","Terrorism","Creates"),("Ben","Hate-Speech","Shares"),
                              ("Jennifer","Sports","Creates"),("Roger","Sports","Likes"),
                              ("Roger","Politics","Likes"),("Tom","Sports","Likes"),
                              ("Tom","Politics","Creates"))).toDF("src","dst","relationship")
// Creating the Graph Frame by passing the vertices and edges data frames
//to the GraphFrame class constructor.
val terrorAnalytics = GraphFrame(vertices,edges)
//You can run degrees on this GraphFrame as follows 
terrorAnalytics.degrees.show()







./bin/spark-shell --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11

//Code for Exploring graphs using GraphFrames section
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.Row
import org.graphframes._

//Code for Constructing a GraphFrame section
val edgesRDD = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Downloads/amzncopurchase/amazon0601.txt")
val schemaString = "src dst"
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false))
val edgesSchema = new StructType(fields)
val rowRDD = edgesRDD.map(_.split("\t")).map(attributes => Row(attributes(0).trim, attributes(1).trim))
val edgesDF = spark.createDataFrame(rowRDD, edgesSchema)
val srcVerticesDF = edgesDF.select($"src").distinct
val destVerticesDF = edgesDF.select($"dst").distinct
val verticesDF = srcVerticesDF.union(destVerticesDF).distinct.select($"src".alias("id"))
edgesDF.count()
verticesDF.count()
val g = GraphFrame(verticesDF, edgesDF)







val x1 = df1.select(df1.col(“similarLines”))
df1.select(df1.col(“similarLines.similar”)).take(5).foreach(println)

val nodesDF = df1.select($"ASIN".alias("id"), $"Id".alias("productId"), $"title", $"ReviewMetaData", 
              $"categories", $"categoryLines", $"group", $"reviewLines", $"salerank", $"similarLines", $"similars")
val edgesDF = df1.select($"ASIN".alias("src"), explode($"similarLines.similar").as("dst"))
val g = GraphFrame(nodesDF, edgesDF)
g.edges.filter(“salesrank < 100”).count()
g.vertices.groupBy(“group”).count().show()






val joinDF  = nodesDF.join(edgesDF)
        .where(nodesDF("id") === edgesDF("src"))
        .withColumn("relationship", 
            when(($"similars" > 4) and ($"categories" <= 3), "highSimilars")
            .otherwise("alsoPurchased"))
val edgesDFR = joinDF.select("src", "dst", "relationship")
val gDFR = GraphFrame(nodesDF, edgesDFR)






val v2 = gDFR.vertices.select("id", "group", "similars")
                      .filter("group = 'Book'")
val e2 = gDFR.edges.filter("relationship = 'highSimilars'")
val g2 = GraphFrame(v2, e2)
val numEHS = g2.edges.count()
val numVHS = g2.vertices.count()
val res1 = g2.find("(a)-[]->(b); (b)-[]->(c); !(a)-[]->(c)")
             .filter("(a.group = c.group) and (a.similars = c.similars)")
val res2 = res1.filter("a.id != c.id")
               .select("a.id", "a.group","a.similars", 
                               "c.id", "c.group", "c.similars") 
res2.count()
res2.show(5)





val paths = g.find("(a)-[e]->(b)")
             .filter("e.relationship = 'highSimilars'")
             .filter("a.group === b.group")
val e2 = paths.select(“e.src”, “e.dst”, “e.relationship”)
val g2 = GraphFrame(gDFR.vertices, e2)
val numEHS = g2.edges.count()





val bfsDF = gDFR.bfs.fromExpr("group = 'Book'")
                    .toExpr("categories < 3")
                    .edgeFilter("relationship != 'alsoPurchased'")
                    .maxPathLength(3).run()
bfsDF.take(2).foreach(println)







