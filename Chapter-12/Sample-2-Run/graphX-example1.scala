import org.apache.spark.{SparkConf, SparkContext}

object grafX_1 {
  def main(args: Array[String]): Unit =
  {
    import org.apache.spark.graphx._


    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sheet")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

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

  }

}