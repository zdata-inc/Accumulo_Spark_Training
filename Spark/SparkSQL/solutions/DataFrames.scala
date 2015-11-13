import sqlContext.implicits._

// use the movies.dat file
val movies_file = "/vagrant/Exercises/Spark/data/ml-1m/movies.dat"

// Define a schema using a case class.
case class Movie(movieId: Int, title: String, genre:String)

// Read in text file 
val movies = sc.textFile(movies_file).map(_.split("::")).map(m => Movie(m(0).toInt, m(1), m(2))).toDF()


// use the ratings.dat file
val ratings_file = "/vagrant/Exercises/Spark/data/ml-1m/ratings.dat"

// Define a schema using a case class.
case class Ratings(userId: Int, movieId: Int, rating: Int)

// Load the ratings file and convert it into a data frame
val ratings = sc.textFile(ratings_file).map(_.split("::")).map(r => Ratings(r(0).toInt, r(1).toInt, r(2).toInt)).toDF()
​
// Load the users file
val users_file = "/vagrant/Exercises/Spark/data/ml-1m/users.dat"

// Define a schema using a case class.
case class Users(userId: Int, gender: String, age: Int, occupation: String, postal: String)

// Load the ratings file and convert it into a data frame
val users = sc.textFile(users_file).map(_.split("::")).map(u => Users(u(0).toInt, u(1), u(2).toInt, u(3), u(4))).toDF()
​


// Display the Schema of all of the DataFrames 
movies.printSchema
ratings.printSchema
users.printSchema

//
// Ratings operations

// Get the average ratings for each user
ratings.groupBy("userId").avg("rating").show()

// Get the average ratings, min rating, and max ratings for each user
ratings.groupBy("userId").agg(avg("rating").alias("average"), min("rating").alias("min"), max("rating").alias("Max")).show()

//
// User operations

// Get all of the teenagers
users.filter(users("age")> 12 && users("age") < 20).show()

// Average age of each gender
users.groupBy("gender").avg("age").show()

//
// Movie operations

movies.join(ratings, movies("movieId").equalTo(ratings("movieId"))).show()

// Average rating for each movie
movies.join(ratings, movies("movieId").equalTo(ratings("movieId"))).groupBy(movies("movieId")).agg(avg("rating").alias("average rating")).show()

// Compare the average ratings for each movie, by gender for users between the ages of 19 and 20, 
users.filter(users("age")>= 20 && users("age") < 40).join(ratings, users("userId").equalTo(ratings("userId"))).join(movies, ratings("movieId").equalTo(movies("movieId"))).groupBy(movies("title"), users("gender")).avg("rating").orderBy("title")show
