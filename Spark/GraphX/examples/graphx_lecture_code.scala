import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Initialize the vertex array
val vertexArray = Array(
  (1L, ("Alice", 28)),
  (2L, ("Bob", 27)),
  (3L, ("Charlie", 65)),
  (4L, ("David", 42)),
  (5L, ("Ed", 55)),
  (6L, ("Fran", 50))
  )

// Initialize the edge array
val edgeArray = Array(
  Edge(2L, 1L, 7),
  Edge(2L, 4L, 2),
  Edge(3L, 2L, 4),
  Edge(3L, 6L, 3),
  Edge(4L, 1L, 1),
  Edge(5L, 2L, 2),
  Edge(5L, 3L, 8),
  Edge(5L, 6L, 3)
  )

// Create an RDD for the vertices
val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)

// Create an RDD for the edges
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

// Build the initial Graph
val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

// Get names of the users that are at least 30 years old
graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect().foreach {
  case (id, (name, age)) => println(s"$name is $age")
}

// Another way to iterate over the users names
for ((id,(name,age)) <- graph.vertices.filter { case (id,(name,age)) => age > 30 }.collect()) {
  println(s"$name is $age")
}

// Get who likes who
for (triplet <- graph.triplets.collect) {
  println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
}

// Define a class to more clearly model the user property
case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

// Create a user Graph
val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }

// Fill in the degree information
val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
  case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
}.outerJoinVertices(initialUserGraph.outDegrees) {
  case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
}

// Print the number of people who like each user: 
for ((id, property) <- userGraph.vertices.collect) {
  println(s"User $id is called ${property.name} and is liked by ${property.inDeg} people.")
}

// Print the names of the users who are liked by the same number of people they like
userGraph.vertices.filter {
  case (id, u) => u.inDeg == u.outDeg
}.collect.foreach {
  case (id, property) => println(property.name)
}

// Find the oldest follower for each user
val oldestFollower: VertexRDD[(String, Int)] = userGraph.mapReduceTriplets[(String, Int)](
  // For each edge send a message to the destination vertex with the attribute of the source vertex
  edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
  // To combine messages take the message for the older follower
  (a, b) => if (a._2 > b._2) a else b
  )

// Display oldest follower for each user
userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
  optOldestFollower match {
    case None => s"${user.name} does not have any followers."
    case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
  }
}.collect.foreach { case (id, str) => println(str) }

// Restrict graph to users 30 and over
val olderGraph = userGraph.subgraph(vpred = (id, user) => user.age >= 30)

// compute the connected components
val cc = olderGraph.connectedComponents

// display the component id of each user:
olderGraph.vertices.leftJoin(cc.vertices) {
  case (id, user, comp) => s"${user.name} is in component ${comp.get}"
}.collect.foreach{ case (id, str) => println(str) }