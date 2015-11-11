// Make a RDD from text file
val textFile = sc.textFile("norway.html")

// Number of items in RDD
textFile.count()

// First item in this RDD
textFile.first()

// A new RDD with a subset of the items
val linesWithEurope = textFile.filter(line => line.contains("Europe"))

// Transformations can chain together transformations and actions
textFile.filter(line => line.contains("Europe")).count()

// Save Results
linesWithEurope.saveAsTextFile("EuropeCounts.txt")