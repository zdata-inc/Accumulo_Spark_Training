// Create some data
val data = 1 to 10000

// Create an RDD from the data
val firstRDD = sc.parallelize(data)

// Perform a transformation and an action on the data
firstRDD.filter(_ < 10).collect()