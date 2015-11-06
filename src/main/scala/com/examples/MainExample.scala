package com.examples

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object MovieLenseExample {

  case class Ratings(UserID: Int, MovieID: Int, Rating: Int, Timestamp: String)
  case class Movies(MovieID: Int, movieName: String, Categories: String)
  case class Users(UserID: Int, Gender: String, Age: Int, Occupation: String, Zipcode: String)

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment

    val conf = new SparkConf()
      .setAppName("MovieLensALS")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // load ratings and movie titles

    val ratings = sc.textFile("hdfs:////user/vagrant/ml-1m/ratings.dat").map(_.split("::")).map(fields =>
      Ratings(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3))).toDF()

    ratings.registerTempTable("ratings")

    val movies = sc.textFile("hdfs:////user/vagrant/ml-1m/movies.dat").map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName, Categories)
      Movies(fields(0).toInt, fields(1), fields(2))
    }.toDF()

    movies.registerTempTable("movies")

    val users = sc.textFile("hdfs:////user/vagrant/ml-1m/users.dat").map { line =>
      val fields = line.split("::")
      // format: (userID, Gender, Age, Occupation, Zipcode)
      Users(fields(0).toInt, fields(1), fields(2).toInt, fields(3), fields(4))
    }.toDF()

    users.registerTempTable("users")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val joins = sqlContext.sql("SELECT * from (SELECT * FROM ratings a join movies b on a.MovieID = b.MovieID limit 1000)a join users b on a.UserID = b.UserID")

    joins.collect().foreach(println)

    var temp = joins.map(df => LabeledPoint(df.getDouble(0), Vectors.dense(df.getDouble(1),df.getDouble(2))))
    temp.take(5)
    
  //  val labeled = joins.map(row => LabeledPoint(row.getDouble(0), row(4).asInstanceOf[Vector]))

    
    /*
    val superJoinLabeled = superJoin.map(line => LabeledPoint(line._2._2._2(3).toDouble,Vectors.dense(
		line.whatever,
		line.whateverElse
      )))
​
    val splits = superJoinLabeled.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
​
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32
​
    val categoricalFeaturesInfo = Map[Int, Int]()
    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    */
   
    /*
    val movies = sc.textFile("hdfs:////user/vagrant/ml-1m/movies.dat").map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName, Categories)
      (fields(0).toInt, (fields(1), fields(2)))      
    }
    
     val users = sc.textFile("hdfs:////user/vagrant/ml-1m/users.dat").map { line =>
      val fields = line.split("::")
      // format: (userID, Gender, Age, Occupation, Zipcode)
      (fields(0).toInt, (fields(1), fields(2), fields(3), fields(4), fields(5)))      
    }

    ratings.join(movies).take(10).foreach(println)
*/
    // clean up
    sc.stop()
  }
}