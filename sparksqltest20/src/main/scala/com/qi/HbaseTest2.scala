package com.qi

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object HbaseTest2 {
  val conf = new SparkConf().setAppName("hbaseTest").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  def readFordHBase() = {
    //配置congiguation 来给newAPIHadoopRDD指明要取哪里连接hbase
    val congiguation = HBaseConfiguration.create()
    //设置要读取的表名
    congiguation.set(TableInputFormat.INPUT_TABLE, "bd20:person")

    val rdd = sc.newAPIHadoopRDD(congiguation, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    //取出数据打印
    rdd.foreach(x => {
      //      println(x._1)
      val result = x._2
      val rowkey = Bytes.toString(result.getRow)
      val age = Bytes.toString(result.getValue("i".getBytes(), "age".getBytes()))
      val gender = Bytes.toString(result.getValue("i".getBytes(), "gender".getBytes()))
      val name = Bytes.toString(result.getValue("i".getBytes(), "name".getBytes()))
      println(s"rowkey:$rowkey,age:$age,gender:$gender,name:$name")
    })
  }

  def writeToHbase() = {
        val rdd = sc.parallelize(List(("2",28,"male","qianqi"),("3",23,"fmale","qsdfsi"),("4",25,"male","sdgsqasfqi"),("5",36,"fmale","dgdgi")))

        val toHBaseRdd = rdd.map(x=>{
          val put  =new Put(Bytes.toBytes(x._1))
          put.addColumn("i".getBytes(),"age".getBytes(),Bytes.toBytes(x._2))
          put.addColumn("i".getBytes(),"gender".getBytes(),x._3.getBytes())
          put.addColumn("i".getBytes(),"name".getBytes(),x._4.getBytes())
          (new ImmutableBytesWritable,put)
        })
        val congiguation = HBaseConfiguration.create()
        congiguation.set(TableOutputFormat.OUTPUT_TABLE, "bd20:person")
        val job = Job.getInstance(congiguation)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        //把job当前
        toHBaseRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

    //    没实现
    //    foreachPartition  //使用foreachpartition实现吧rdd的数据写入hbase中
    //    会造成高并发写入
//    val rdd1 = sc.parallelize(List(("10", 28, "male", "qianqi"), ("11", 23, "fmale", "qsdfsi"), ("12", 25, "male", "sdgsqasfqi"), ("13", 36, "fmale", "dgdgi")))
//
//    rdd1.foreachPartition(x => {
//
//      val hbaseKV= x.map(record => {
//        val put = new Put(Bytes.toBytes(record._1))
//        put.addColumn("i".getBytes(), "age".getBytes(), Bytes.toBytes(record._2))
//        put.addColumn("i".getBytes(), "gender".getBytes(), record._3.getBytes())
//        put.addColumn("i".getBytes(), "name".getBytes(), record._4.getBytes())
//        (new ImmutableBytesWritable, put)
//      })
//
//      val toHbaseRdd=sc.parallelize(hbaseKV.toSeq)
//
//      val congiguation = HBaseConfiguration.create()
//      congiguation.set(TableOutputFormat.OUTPUT_TABLE, "bd20:person")
//      val job = Job.getInstance(congiguation)
//      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
//      job.setOutputValueClass(classOf[Put])
//      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
//      toHbaseRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
//    })

  }

  def main(args: Array[String]): Unit = {
//    readFordHBase
        writeToHbase
  }
}
