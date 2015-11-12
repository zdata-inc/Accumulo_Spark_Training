package com.examples

// Exercise:
// - load ratings, movies, users data files from the 1 million row MovieLens data
// - do a statistical summary on ratings

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._

object MovieLensStats {

  def main(arg: Array[String]) {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    var logger = Logger.getLogger(this.getClass())

    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample <path-to-files> <output-path>")
      System.exit(1)
    }
    
    // Set the job name
    val jobName = "MovieLensPlusMLlib"

    // Configure the SparkContext
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)
    val outputPath = arg(1)

    // STEP 1: Load the files
   
    // UserID::MovieID::Rating::Timestamp
    val ratArrays = sc.textFile(new File(pathToFiles, "ratings.dat").toString).map(_.split("::"))
    // MovieID::Title::Genres
    val movArrays = sc.textFile(new File(pathToFiles, "movies.dat").toString).map(_.split("::"))
    // UserID::Gender::Age::Occupation::Zip-code
    val usrArrays = sc.textFile(new File(pathToFiles, "users.dat").toString).map(_.split("::"))

    // ALL RATINGS 
    // convert to dense vectors with just ratings in there
    val ratvecs = ratArrays.map(x => Vectors.dense(x(2).toDouble))
    
    // statistical summary of ratings
    val ratsum: MultivariateStatisticalSummary = Statistics.colStats(ratvecs)
    println(ratsum.mean)
    println(ratsum.variance) 

    // FOR A MOVIE by name
    val cluelessRatings = ratArrays.map( x=> (x(1),x)) // key ratings by movie id
      .join(movArrays.map( x=> (x(0),x))) // RDD (movieId, (ratings,movies))
      .filter(row => row._2._2(1).startsWith("Clueless")) // filter out other movies
      .map(row => Vectors.dense(row._2._1(2).toDouble)) // convert to dense vector
    print(cluelessRatings.take(2))
    
    
    val movsum: MultivariateStatisticalSummary = Statistics.colStats(cluelessRatings)
    
    // Display the statistics
    println(movsum.mean) 
    println(ratsum.variance) 
    
  }
}
