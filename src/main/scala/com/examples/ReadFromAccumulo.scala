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
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Collection => JCollection}

object ReadFromAccumulo {

  // whenever a method calls for a Java Collection of type Range, we can just pass a single range
  // and this will auto convert it for us
  implicit def singletonRange(r: Range): JCollection[Range] = java.util.Collections.singleton(r)

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

