// Load the page names 
val pageNames = sc.textFile("pages.txt")

// Convert the page names into a map
val pageMap = pageNames.map(l => (l.split(",")(0), l.split(",")(1))).collect().toMap

// Broadcast the pageMapRDD
val bc = sc.broadcast(pageMap) 

// Load the page visits 
val visits = sc.textFile("visits.txt")

// Convert the visit into a map
val visitMap = visits.map(l => (l.split(",")(0), l.split(",")(1)))

// Join the two data sources
val joined = visitMap.map(v => (v._1, (bc.value(v._1), v._2)))

// Collect the join
joined.collect()