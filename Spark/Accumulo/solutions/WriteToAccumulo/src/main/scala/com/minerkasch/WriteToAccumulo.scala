package com.minerkasch;
import java.io.File

import com.google.common.io.Files
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.{BatchWriterConfig, ClientConfiguration}
import org.apache.accumulo.core.client.mapreduce.{AccumuloOutputFormat, InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Value, Key, Range}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Collection => JCollection}


object WriteToAccumulo {
  
  // Set the application name
  val APP_NAME = "AccumuloReader"
  
  // Set the application
  val USER_NAME = "root"
  val USER_PASS = "password"
  val INSTANCE = "hdp-accumulo-instance"
  val ZOOKEEPERS = "localhost:2181"

  // Set the table name
  val TABLE_NAME = "SparkData"
  
  def main(args: Array[String]) = {

    // Ensure the correct number of command line args were specified
    if (args.length != 1) {
      System.err.println("Usage: WriteToAccumulo <input_file>")
      System.exit(1)
    }
    
    // Get the input file
    val inputFile = args(0)
    
    // Set configurations for the job
    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Key], classOf[Value]))

    // Initialize the SparkContext
    val sc = new SparkContext(conf)
    
    // Load the input file
    val movies = sc.textFile(new File(inputFile, "movies.dat").toString).map(_.split("::"))
    
    // Create mutations from the array
    val mutations = movies.map { line =>
      val m = new Mutation(line(0).toString)
      m.put(line(2), "", line(1))
      (new Text(TABLE_NAME), m)
    }
    
    // Initialize the job
    val job = new Job()
    
    // Set the Accumulo output format
		AccumuloOutputFormat.setConnectorInfo(job, USER_NAME, new PasswordToken(USER_PASS));
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setDefaultTableName(job, TABLE_NAME);
		AccumuloOutputFormat.setZooKeeperInstance(job, 
		  new ClientConfiguration()
        .withInstance(INSTANCE)
        .withZkHosts(ZOOKEEPERS));
    
    // Save the mutations RDD to Accumulo
    mutations.saveAsNewAPIHadoopFile("/", 
        classOf[Text], 
        classOf[Mutation], 
        classOf[AccumuloOutputFormat], 
        job.getConfiguration)
        
  }
}