package com.examples

// Exercise:
// - load ratings, movies, users data files from the 1 million row MovieLens data
// - join the three RDDs using RDD.join
// - build a regression tree to predict ratings based on user profile stats (age, gender, occupation)
// - evaluate it

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._

object RegressionTree {

  def main(arg: Array[String]) {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    var logger = Logger.getLogger(this.getClass())

    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample <path-to-files>")
      System.exit(1)
    }

    // Set the Job name
    val jobName = "MovieLensPlusMLlib"

    // Configure the SparkContext
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)

    // Load the data
    
    // UserID::MovieID::Rating::Timestamp
    val ratArrays = sc.textFile(new File(pathToFiles, "ratings.dat").toString)
    
    // MovieID::Title::Genres
    val movArrays = sc.textFile(new File(pathToFiles, "movies.dat").toString)
    
    // UserID::Gender::Age::Occupation::Zip-code
    val usrArrays = sc.textFile(new File(pathToFiles, "users.dat").toString)
    
    // Produce an RDD with each element as (User ID, User)
    val usrById = usrArrays.map { row => 
      val line = row.split("::")
      (line(0),line) 
    }
    
    // Produce and RDD with each element as (Movie ID, Movie)
    val movById = movArrays.map { row =>
      val line = row.split("::")
      (line(0),line) }
    
    // Create a ratings joined with user (User ID, (User, Ratings))
    val ratJoinUsr = usrById.join(ratArrays.map { row => 
      val line = row.split("::")
      (line(0),line) 
    })
      
    //  superJoin is (movieid,(movieStArray,(usrStArray,ratStArray)))
    val superJoin  = movById.join(ratJoinUsr.map { line => (line._2._2(1),line._2) })

    // map the data into a LabeledPoint. sorry about the mess, this could be cleaner.
    // line._2._1 is movies
    // line._2._2._1 is users
    // line._2._2._2 is ratings
    val superJoinLabeled = superJoin.map(line => LabeledPoint(// TODO set the labeled point to the rating
        ,Vectors.dense(
        //TODO set the users age, // this is user age
        // TODO set the users occupation , // this is user occupation
        (if (line._2._2._1(1)=="M") 1.0 else 0.0) // this is user gender - convert to double
        )))

    // Split the data into training and test sets
    val splits = superJoinLabeled.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Set the properties for our Regression tree
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32

    // Use 2 categorical features
    // occupation has 21 possible values and gender has 2 
    // TODO: Set 2 categorical features bease on our labeled point
    // 1 -> 21, 2 -> 2
    val categoricalFeaturesInfo = Map[Int, Int](//TODO: Set 2 )
    
    // Train the regression tree
    val model = // TODO Train the regressor

    // Make prediction
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    
    // Evaluate the performance on the test set
    // Error is sum of abs value of difference between prediction and actual, divided by size
    val testErr = labelAndPreds.map(r => Math.abs(r._1-r._2)).reduce(_ + _) / testData.count()
    
    // Display the results
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

  }
}
