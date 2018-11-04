# Code for Chapter 12

# rin within the Anaconda Python shell:
pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11

vertices = sqlContext.createDataFrame([
  ("a", "Alice", 34), ("b", "Bob", 36),
  ("c", "Charlie", 30), ("d", "David", 29),
  ("e", "Esther", 32), ("f", "Fanny", 36),
  ("g", "Gabby", 60)],  
 ["id", "name", "age"])
edges = sqlContext.createDataFrame([
  ("a", "b", "friend"), ("b", "c", "follow"),
  ("c", "b", "follow"), ("f", "c", "follow"),
  ("e", "f", "follow"), ("e", "d", "friend"),
  ("d", "a", "friend"), ("a", "e", "friend")],
 ["src", "dst", "relationship"])
g = GraphFrame(vertices, edges)
display(g.vertices)
display(g.inDegrees)
display(g.degrees)

youngest = g.vertices.groupBy().min("age")
#29

numFollows = g.edges.filter("relationship = 'follow'").count()


print "The number of follow edges is", numFollows
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")


paths = g.find("(a)-[e]->(b)")\
  .filter("e.relationship = 'follow'")\
  .filter("a.age < b.age")
# The `paths` variable contains the vertex information, which we can extract:
e2 = paths.select("e.src", "e.dst", "e.relationship")

# In Spark 1.5+, the user may simplify the previous call to:
# val e2 = paths.select("e.*")

# Construct the subgraph
g2 = GraphFrame(g.vertices, e2)




filteredPaths = g.bfs(
  fromExpr = "name = 'Esther'",
  toExpr = "age < 32",
  edgeFilter = "relationship != 'friend'",
  maxPathLength = 3)
display(filteredPaths)
result = g.connectedComponents()
result = g.labelPropagation(maxIter=5)
display(result)


results = g.pageRank(resetProbability=0.15, tol=0.01)
display(results.vertices)



# Run PageRank for a fixed number of iterations.
g.pageRank(resetProbability=0.15, maxIter=10)

results = g.shortestPaths(landmarks=["a", "d"])
display(results)



g.edges.filter(“salesrank < 100”).explain()
