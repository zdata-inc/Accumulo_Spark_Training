import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, "followers.txt")

// find the connected components
val cc = graph.connectedComponents().vertices

// run pageRank().verticies
val ranks = graph.pageRank(0.0001).vertices

//  load the user names with vertex id
val users = sc.textFile("users.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}

// join the connected components with the usernames
val ccByUsername = users.join(cc).map {
  case (id, (username, cc)) => (username, cc)
}

// join the ranks with the usernames
val ranksByUsername = users.join(ranks).map {
  case (id, (username, rank)) => (username, rank)
}

// print the result
println(ccByUsername.collect().mkString("\n"))
println(ranksByUsername.collect().mkString("\n"))
