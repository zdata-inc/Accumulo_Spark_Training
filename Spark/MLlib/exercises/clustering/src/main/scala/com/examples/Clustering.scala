package com.examples

import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Random
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

object Clustering {

  // Generate a data file
  def generateFile(n: Int,rows: Int,out: File) {
    var centroids = Array((0,0))
    val rng = new Random()
    var x = 0
    for ( x <- 1 to n)
      centroids = centroids :+ (x*4,x*2)
    // Display the number of centroids
    println(centroids.length)
    val strm = new BufferedWriter(new FileWriter(out));
    for ( x <- 1 to rows) {
      val c = centroids(rng.nextInt(n))
      val x = (rng.nextGaussian()+c._1)
      val y = (rng.nextGaussian()+c._2)
      strm.write(x + "," + y + "\r\n")
    }
    strm.close()
  }
  
  def main(arg: Array[String]) {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    var logger = Logger.getLogger(this.getClass())

    if (arg.length == 1) {
      val rng = new Random()
      val k = 3+rng.nextInt(3)
      generateFile(k,2000,new File(arg(0),"clustering-data.txt"))
      System.exit(1)
    } else if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample k <path-to-files>")
      System.exit(1)
    }

    // Set the job name
    val jobName = "Clustering"

    // Configure the SparkContext
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    // Get the number of clusters
    val numClusters = arg(0).toInt
    val pathToFiles = arg(1)

    // Load the clustering data
    val data = sc.textFile(new File(pathToFiles, "clustering-data.txt").toString)

    // Parse the data (and cache it)
    val parsedData = data.map(s=> //TODO: create a dense vector ).cache()
    
    // start with k=2
    val numIterations = 20

    // Train the model
    val clusters = //TODO: Call Kmeans to train the data
    
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = //TODO: compute the cost of the model
    
    println("Within Set Sum of Squared Errors = " + WSSSE)

  }
}
