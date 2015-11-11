file:///vagrant/Accumulo_Spark_Training/Spark/data/mapReduce.java

// Initialize an accumulator to count the number of comments
val comments = sc.accumulator(0)

// Load the map reduce file
val mapReduce = sc.textFile("mapReduce.java")
val mapReduce = sc.textFile("file:///vagrant/Accumulo_Spark_Training/Spark/data/mapReduce.java")

// 
val sourceCode = mapReduce.filter(line => {
   if (line.contains("//")) {
      comments += 1
      false
   } else {
      true
   }
})

// Output the number of lines of source code
sourceCode.count()

// Output the number of comments
comments
