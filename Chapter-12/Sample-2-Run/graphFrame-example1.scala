import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.{GraphFrame, examples}

object grframe1 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sheet")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val v = sqlContext.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    // Edge DataFrame
    val e = sqlContext.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")

    val g = GraphFrame(v, e)

    //g.degrees.show()

    g.vertices.show()

    g.inDegrees.show()

    g.degrees.show()

    //val motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
    //motifs.show()



    val f: GraphFrame = examples.Graphs.friends  // get example graph
    val motifss = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifss.show()

    // More complex queries can be expressed by applying filters.
    motifss.filter("b.age > 30").show()

  }
}