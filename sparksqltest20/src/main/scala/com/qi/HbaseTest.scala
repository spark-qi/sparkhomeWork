package com.qi

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object HbaseTest {
  val conf = new SparkConf().setAppName("hbasetest").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  def readFromHbase()={
    val configration = HBaseConfiguration.create()
    //设置需要读取的表名
    configration.set(TableInputFormat.INPUT_TABLE,"bd20:wc")
    //如果需要限定扫描的数据,可以使用

    val rdd = sc.newAPIHadoopRDD(configration,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    //取出打印数据
    rdd.foreach(x=>{
      println(x._1)
      val result = x._2
      val rowkey = Bytes.toString(result.getRow)
      val age = Bytes.toString(result.getValue("i".getBytes(),"age".getBytes()))
      val count = Bytes.toString(result.getValue("i".getBytes(),"count".getBytes()))
      println(s"rowkey:$rowkey,i:age:$age,count:$count")
    })
  }

  def writeToHbase()={
    val rdd = sc.parallelize(List(("001","qqq",23),("002","www",25),("003","eee",33)))
    val toHBaseRdd = rdd.map(x=>{
      val put = new Put(Bytes.toBytes(x._1))
      put.addColumn("i".getBytes(),"name".getBytes(),x._2.getBytes())
      put.addColumn("i".getBytes(),"age".getBytes(),Bytes.toBytes(x._3))
      (new ImmutableBytesWritable,put)
    })
    val configration = HBaseConfiguration.create()
    configration.set(TableOutputFormat.OUTPUT_TABLE,"bd20:hbase")
    val job = Job.getInstance(configration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    //把job当做配置参数传入 中,把数据保存到HBase中
    toHBaseRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  //使用foreachpartition实现吧rdd的数据写入hbase中
  //会造成高并发写入
  def foreachWriteToHBase()={
    val rdd = sc.parallelize(List(("004","zzzzz",23),("004","xxxxx",25),("005","ccccc",33)))
    rdd.foreachPartition(x=>{
      val configuration = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(configuration)
      val table = connection.getTable(TableName.valueOf("bd20:hbase"))
      //调用table的put方法进行单个或者批量的保存数据到HBase中
    })

  }



  def main(args: Array[String]): Unit = {
    readFromHbase()
//    writeToHbase()

  }

}
