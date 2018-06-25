import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._

object BulkLoadT {
  def main(args: Array[String]): Unit = {
    System.setProperty("user.name", "hbase")
    System.setProperty("HADOOP_USER_NAME", "hbase")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","192.168.0.83")
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    val tableN = "cs"
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableN);
    val table = new HTable(conf,tableN)
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(job,table)
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("toHbase").set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    val sc = new SparkContext(sparkConf)
    val num = sc.parallelize(1 to 10)
    val rdd = num.map(x=>{
      val kv:KeyValue = new KeyValue(Bytes.toBytes(x),"info".getBytes(),"name".getBytes(),"chensi".getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(x)),kv)
    })
    rdd.saveAsNewAPIHadoopFile("/tmp/cs",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat],conf)
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("/tmp/cs"),table)
//    rdd.saveAsNewAPIHadoopFile("/tmp/cs2",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat],job.getConfiguration)
    sc.stop()
  }
}
