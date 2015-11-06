import java.io.File

import com.google.common.io.Files
import org.apache.accumulo.core.client.{BatchWriterConfig, ClientConfiguration}
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Value, Key, Range}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.minicluster.impl.{MiniAccumuloConfigImpl, MiniAccumuloClusterImpl}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Collection => JCollection}

object ReadFromAccumulo {

  import Common._

  def main(args: Array[String]) = {
    var cluster: MAC = null
    try {
      cluster = MAC()
      cluster.start()

      // Put data in a table here
      load_data(cluster)


      // let's set up what we'd want a job to look like when reading from Accumulo
      val job = cluster.job()
      InputFormatBase.setRanges(job, new Range())
      InputFormatBase.setInputTableName(job, "demo")


      // here we'll initialize spark
      val sc = new SparkContext(new SparkConf().setAppName("Training Demo").setMaster("local"))

      val rdd = sc.newAPIHadoopRDD(job.getConfiguration,
        classOf[AccumuloInputFormat],
        classOf[Key],
        classOf[Value])

      println(rdd.count())

    } finally {
      if(cluster != null) cluster.stop()
    }
  }
}

object ReadOfflineTable {
  import Common._

  def main(args: Array[String]) = {
    var cluster: MAC = null
    try {
      cluster = MAC()
      cluster.start()

      // Put data in a table here
      load_data(cluster)

      //Let's assume the "demo" table is being actively written to and queried. We don't want our bulk analysis to
      //have a big impact on those other clients, so we can clone and offline the table. Cloning is a lightweight
      //operation that copies the metadata of a table at a particular time (like snapshotting), so no actual data
      //on disk is copied. Offlining the table prevents any activity from occuring on the table, and allows for reads
      //to bypass the TServer and read directly off of disk.
      cluster.offline_table("demo", "demo_offline")

      // let's set up what we'd want a job to look like when reading from Accumulo
      val job = cluster.job()
      InputFormatBase.setRanges(job, new Range())
      InputFormatBase.setInputTableName(job, "demo_offline")


      // here we'll initialize spark
      val sc = new SparkContext(new SparkConf().setAppName("Training Demo").setMaster("local"))

      val rdd = sc.newAPIHadoopRDD(job.getConfiguration,
        classOf[AccumuloInputFormat],
        classOf[Key],
        classOf[Value])

      println(rdd.count())

    } finally {
      if(cluster != null) cluster.stop()
    }
  }
}


object ReadTableThenRepartitionRDD {
  import Common._

  def main(args: Array[String]) = {
    var cluster: MAC = null
    try {
      cluster = MAC()
      cluster.start()

      // Put data in a table here
      load_data(cluster)

      cluster.offline_table("demo", "demo_offline")

      // let's set up what we'd want a job to look like when reading from Accumulo
      val job = cluster.job()
      InputFormatBase.setRanges(job, new Range())
      InputFormatBase.setInputTableName(job, "demo_offline")


      // here we'll initialize spark
      // note that the spark configuration needs to have the Key and Value classes serializable by Kryo
      val sc = new SparkContext(new SparkConf().setAppName("Training Demo").setMaster("local").registerKryoClasses(Array(classOf[Key], classOf[Value])))

      val rdd = sc.newAPIHadoopRDD(job.getConfiguration,
        classOf[AccumuloInputFormat],
        classOf[Key],
        classOf[Value])

      // some times we can gain speed by repartitioning the data to increase parallelism
      val repartitioned = rdd.repartition(4)

      // we can even cahce this RDD in memory or on disk. we can also store it in both. doing the persistence
      // allows for downstream operations to be able to recompute from this point, rather than going back to Accumulo
      val persisted = repartitioned.persist(StorageLevel.MEMORY_AND_DISK)

      def square_key(e: (Key, Value)): Int = {
        val int = e._1.getRow.toString.toInt
        return (int * int)
      }

      val squares = persisted.map(square_key)

      squares.take(10).foreach(println)

    } finally {
      if(cluster != null) cluster.stop()
    }
  }
}

class MAC(val accumulo: MiniAccumuloClusterImpl) {
  def start(): Unit = {
    accumulo.start()
  }

  def stop(): Unit = {
    accumulo.stop()
  }

  def set_instance(job:Job): Job = {
    AbstractInputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(accumulo.getInstanceName).withZkHosts(accumulo.getZooKeepers))
    return job
  }

  def set_connection(job:Job): Job = {
    AbstractInputFormat.setConnectorInfo(job, "root", new PasswordToken("minerkasch"))
    return job
  }

  def set_authorizations(job: Job): Job = {
    AbstractInputFormat.setScanAuthorizations(job, new Authorizations)
    return job
  }

  def job(): Job = {
    val job = new Job()
    set_instance(job)
    set_connection(job)
    set_authorizations(job)
    return job
  }

  def offline_table(table: String, offline_table_name: String): Unit = {
    val con = accumulo.getConnector("root", "minerkasch")
    con.tableOperations().clone(table, offline_table_name, true, new java.util.HashMap[String, String], new java.util.HashSet[String])
  }
}

object MAC {
  def apply(work_dir: File): MAC = {
    return new MAC(new MiniAccumuloClusterImpl(new MiniAccumuloConfigImpl(work_dir, "minerkasch")))
  }

  def apply(): MAC = {
    val dir = Files.createTempDir()
    dir.deleteOnExit()
    return apply(dir)
  }
}

object Common {
  // whenever a method calls for a Java Collection of type Range, we can just pass a single range
  // and this will auto convert it for us
  implicit def singletonRange(r: Range): JCollection[Range] = java.util.Collections.singleton(r)

  def load_data(cluster: MAC): Unit = {
    val con = cluster.accumulo.getConnector("root", new PasswordToken("minerkasch"))
    con.tableOperations().create("demo")
    val bw = con.createBatchWriter("demo", new BatchWriterConfig())
    for(x <- 1 to 100) {
      val m = new Mutation(x.toString)
      m.put("", "", "hello")
      bw.addMutation(m)
    }
    bw.close()
  }
}

