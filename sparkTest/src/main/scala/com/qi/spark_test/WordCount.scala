package com.qi.spark_test

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  //创建配置信息,本地运行
  //    val conf=new SparkConf().setAppName("wordcount").setMaster("local[*]")
  //集群运行
    val conf = new SparkConf().setAppName("wordcount").setMaster("spark://master:7077")
  //standalone模式
  //val conf = new SparkConf().setAppName("wordcount")


  //创建spark的driver对象SparkContext
  //  val sc = new SparkContext(conf)

  val sc = SparkContext.getOrCreate(conf)

  //sc.addJar("D:\\ScalaWorkSpace\\sparkTest\\out\\artifacts\\sparkTest_jar\\sparkTest.jar")

  //wordcount过程
  def wc(): Unit = {

    val rdd = sc.textFile("/flumetest/henan/FlumeData.1521144537069")
    //数据处理过程
    val wordRdd = rdd.flatMap(x => x.split("---"))
    val kvRdd = wordRdd.map(x => (x, 1))
    val result = kvRdd.reduceByKey((x1, x2) => x1 + x2)
    //    把数据处理结果保存到hdfs上
    //        val result=rdd.flatMap(_.split("\\s+")).map((_,1)).reduce(_+_)
    result.saveAsTextFile("/spartWcbd20-9")
    sc.stop()
  }

  def main(args: Array[String]): Unit = {

    wc()
  }

}
