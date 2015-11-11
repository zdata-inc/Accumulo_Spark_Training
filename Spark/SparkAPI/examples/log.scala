// Read data from a file
val lines = sc.textFile("log.txt")

// Get any lines that start with "ERROR"
val errors = lines.filter(_.startsWith("ERROR"))

// Get the error messages
val messages = errors.map(_.split("\t")).map(r => r(1))

// Cache the RDD
messages.cache()

// Actions on the messages RDD
messages.filter(_.contains("mysql")).count()
messages.filter(_.contains("php")).count()