val lines = sc.textFile("log.txt")

// transformed RDDs
val errors = lines.filter(_.startsWith("ERROR"))
val messages = errors.map(_.split("\t")).map(r => r(1))
messages.cache()

// actions
messages.filter(_.contains("mysql")).count()
messages.filter(_.contains("php")).count()



val messages = errors.map(_.split("\t")).map(r => r(1))
messages.cache()

messages.filter(_.contains("mysql")).count()
messages.filter(_.contains("php")).count()
