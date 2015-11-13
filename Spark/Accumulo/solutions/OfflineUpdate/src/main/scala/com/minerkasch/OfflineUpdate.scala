package com.minerkasch;
import java.io.File

import com.google.common.io.Files
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.{BatchWriterConfig, ClientConfiguration}
import org.apache.accumulo.core.client.mapreduce.{AccumuloOutputFormat, AbstractInputFormat, InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Value, Key, Range}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Collection => JCollection}


object OfflineUpdate {
  
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
  val OFFLINE_TABLE_NAME = "SparkDataOffline" 
  val NEW_TABLE = "SparkInverted"
  
  def main(args: Array[String]) = {

    // Create a connection to accumulo
    val zooKeeperInstance = new ZooKeeperInstance(INSTANCE, ZOOKEEPERS);
    val connector = zooKeeperInstance.getConnector(USER_NAME,
				new PasswordToken(USER_PASS.getBytes()));
    
    // Take the table offline
    connector.tableOperations().clone(TABLE, 
            OFFLINE_TABLE_NAME,
            true, 
            new java.util.HashMap[String, String], 
            new java.util.HashSet[String])
    
    // Initialize the job
    var job = new Job()
    
    // Set the Accumulo input format
    AbstractInputFormat.setZooKeeperInstance(job,
      new ClientConfiguration()
        .withInstance(INSTANCE)
        .withZkHosts(ZOOKEEPERS))
    AbstractInputFormat.setConnectorInfo(job, USER_NAME, new PasswordToken(USER_PASS))
    AbstractInputFormat.setScanAuthorizations(job, new Authorizations)

    // Set up the job to read from an Accumulo Table
    InputFormatBase.setRanges(job, new Range())
    InputFormatBase.setInputTableName(job, OFFLINE_TABLE_NAME)

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
    
    val invertedTable = sparkTable.map { line =>
      val m = new Mutation(line._2.toString)
      m.put(line._1.getRow.toString, "", "")
      (new Text(NEW_TABLE), m)
    }
    
    // Initialize the job
    job = new Job()
    
    // Set the Accumulo output format
		AccumuloOutputFormat.setConnectorInfo(job, USER_NAME, new PasswordToken(USER_PASS));
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setDefaultTableName(job, NEW_TABLE);
		AccumuloOutputFormat.setZooKeeperInstance(job, 
		  new ClientConfiguration()
        .withInstance(INSTANCE)
        .withZkHosts(ZOOKEEPERS));
    
    // Save the mutations RDD to Accumulo
    invertedTable.saveAsNewAPIHadoopFile("/", 
        classOf[Text], 
        classOf[Mutation], 
        classOf[AccumuloOutputFormat], 
        job.getConfiguration)
    
    // Delete the offline table
    connector.tableOperations().delete(OFFLINE_TABLE_NAME)
  }
}