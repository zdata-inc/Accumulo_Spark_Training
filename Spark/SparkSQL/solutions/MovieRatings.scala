import sqlContext.implicits._

// use the movies.dat file
val movies_file = "ml-1m/movies.dat"

// Define a schema using a case class.
case class Movie(movieId: Int, title: String, genre:String)

// Read in text file 
val movies = sc.textFile(movies_file).map(_.split("::")).map(m => Movie(m(0).toInt, m(1), m(2))).toDF()

// Register it as temp table
movies.registerTempTable("movies")

// SQL statements can be run with sqlContext
val res = sqlContext.sql("SELECT title FROM movies WHERE genre LIKE '%Action%'")

// use the ratings.dat file
val ratings_file = "ml-1m/ratings.dat"

// Define a schema using a case class.
case class Ratings(userId: Int, movieId: Int, rating: Int)

// Load the ratings file and convert it into a data frame
val ratings = sc.textFile(ratings_file).map(_.split("::")).map(r => Ratings(r(0).toInt, r(1).toInt, r(2).toInt)).toDF()
​
ratings.registerTempTable("ratings")
val res = sqlContext.sql("SELECT * FROM ratings WHERE rating = 5")

// Some fun with joins
val goodmovies = sqlContext.sql("""
   SELECT m.title, r.rating
   FROM movies AS m
   JOIN ratings AS r ON m.movieId = r.movieId
   WHERE r.rating == 5
   """)

// The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
goodmovies.map(t => "\n---\nMovie: " + t(0) + "\nRating: " + t(1)).collect().foreach(println)