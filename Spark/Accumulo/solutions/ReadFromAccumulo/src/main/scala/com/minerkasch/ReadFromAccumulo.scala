package com.minerkasch;
import java.io.File

import com.google.common.io.Files
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.{BatchWriterConfig, ClientConfiguration}
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Value, Key, Range}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Collection => JCollection}


object ReadFromAccumulo {
  
  // Enable new Range() to return Collection[Range]
  implicit def singletonRange(r: Range): JCollection[Range] = java.util.Collections.singleton(r)
  
  // Set the application name
  val APP_NAME = "AccumuloReader"
  
  // Set the application
  val USER_NAME = "root"
  val USER_PASS = "password"
  val INSTANCE = "hdp-accumulo-instance"
  val ZOOKEEPERS = "localhost:2181"

  // Set the table name
  val TABLE = "SparkData"
  
  def main(args: Array[String]) = {

    // Initialize the job
    val job = new Job()
    
    // Set the Accumulo input format
    AbstractInputFormat.setZooKeeperInstance(job,
      new ClientConfiguration()
        .withInstance(INSTANCE)
        .withZkHosts(ZOOKEEPERS))
    AbstractInputFormat.setConnectorInfo(job, USER_NAME, new PasswordToken(USER_PASS))
    AbstractInputFormat.setScanAuthorizations(job, new Authorizations)

    // Set up the job to read from an Accumulo Table
    InputFormatBase.setRanges(job, new Range())
    InputFormatBase.setInputTableName(job, TABLE)

    // Set configurations for the job
    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Key], classOf[Value]))

    // Initialize the SparkContext
    val sc = new SparkContext(conf)

    // Read from Accumulo
    val sparkTable = sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[AccumuloInputFormat],
      classOf[Key],
      classOf[Value])

    // Cache the TwitterTable  
    sparkTable.cache()
    
    // Display the results
    println("NumRecords: " + sparkTable.count())
    
    // Get the first record
    val firstRow = sparkTable.first()
    
    // Display the results
    println("First Record: " + firstRow)
    println("key: " + firstRow._1.getRow.toString.toLong)
    println("cf: " + firstRow._1.getColumnFamily.toString)
    println("cq: " + firstRow._1.getColumnQualifier.toString)
    println("value: " + firstRow._2)

    // Display 10 Comedys
    val messages = sparkTable.filter(_._1.getColumnFamily.toString.contains("Comedy"))
                               .map(movie => (movie._2.toString))
    
    // Take 10 messages and display them
    messages.take(10).foreach(println)
  }
}