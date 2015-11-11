// Load the file with people data
val inputFile = "people.dat"

// Define a schema using a case class.
case class Person(name: String, age: Int)

// Read in a text file and convert it to a data frame
val people = sc.textFile(inputFile).map(_.split(",")).map(p => Person(p(0),p(1).toInt)).toDF()

// Convert the data frame into a temp table
people.registerTempTable("people")

// Select people that are in their 20's
sqlContext.sql("SELECT name FROM people WHERE age > 19 AND age < 30").collect()