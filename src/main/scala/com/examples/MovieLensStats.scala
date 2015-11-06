package com.examples

// Exercise:
// - load ratings, movies, users data files from the 1 million row MovieLens data
// - do a statistical summary on ratings
// - build a regression tree to predict ratings based on 

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
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

    val jobName = "MovieLensPlusMLlib"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)
    val outputPath = arg(1)
    // case class Rating(userId: Int, movieId: Int, rating: Int)
    // failboat
    // val ratings = sc.textFile(new File(arg(0), "ratings.dat").toString).map(_.split("::")).map(r => Rating(r(0).toInt, r(1).toInt, r(2).toInt)).toDF()

    // load the stuff
    val ratArrays = sc.textFile(new File(pathToFiles, "ratings.dat").toString).map(_.split("::"))
    val movArrays = sc.textFile(new File(pathToFiles, "movies.dat").toString).map(_.split("::"))
    val usrArrays = sc.textFile(new File(pathToFiles, "users.dat").toString).map(_.split("::"))
    
    // statistical summary of ratings
    val ratvecs = ratArrays.map(x => Vectors.dense(x(2).toDouble))
    val ratsum: MultivariateStatisticalSummary = Statistics.colStats(ratvecs)
    println(ratsum.mean) 
    println(ratsum.variance) 
    println(ratsum.numNonzeros)

    // join all three RDDs    
    val usrById = usrArrays.map { line => (line(0),line) }
    val movById = movArrays.map { line => (line(0),line) }
    val ratJoinUsr = usrById.join(ratArrays.map { line => (line(0),line) })
    // superjoin is (movieid,(movieStArray,(usrStArray,ratStArray)))
    val superJoin  = movById.join(ratJoinUsr.map { line => (line._2._2(1),line._2) })
    
    println(superJoin.take(1))
    
    // pull the data into a LabeledPoint
    // line._2._1 is movies
    // line._2._2._1 is users
    // line._2._2._2 is ratings
    val superJoinLabeled = superJoin.map(line => LabeledPoint(line._2._2._2(3).toDouble,Vectors.dense(
        line._2._2._1(2).toDouble, // this is user age
        line._2._2._1(3).toDouble, // this is user occupation
        (if (line._2._2._1(1)=="M") 1.0 else 0.0) // this is user gender
        )))

    val splits = superJoinLabeled.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    println(superJoinLabeled.take(1))
    
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32

    val categoricalFeaturesInfo = Map[Int, Int](1->21,2->2)
    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    // if you want
    // model.save(sc, "myModelPath")
    
  }
}
