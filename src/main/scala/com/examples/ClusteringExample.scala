package com.examples

// Exercise:
// - load clustering data
// - do a statistical summary on ratings

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

object ClusteringExample {

  //
  def generateFile(n: Int,rows: Int,out: File) {
    var centroids = Array((0,0))
    val rng = new Random()
    var x = 0
    for ( x <- 1 to n)
      centroids = centroids :+ (x*4,x*2)
    println(centroids)
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
    } else if (arg.length < 3) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample k <path-to-files> <output-path>")
      System.exit(1)
    }

    val jobName = "ClusteringExample"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val k = arg(0).toInt
    val pathToFiles = arg(1)
    val outputPath = arg(2)

    // load the stuff
    val unproc = sc.textFile(new File(pathToFiles, "clustering-data.txt").toString)

    // process the data (and cache it)
    val proc = unproc.map(s=> Vectors.dense(s.split(",").map(_.toDouble))).cache()
    
    // start with k=2
    val i = 20

    // train the model
    val clusters = KMeans.train(proc, k, i)
    
    // evaluate
    val WSSSE = clusters.computeCost(proc)
    println("Within Set Sum of Squared Errors = " + WSSSE)

  }
}
