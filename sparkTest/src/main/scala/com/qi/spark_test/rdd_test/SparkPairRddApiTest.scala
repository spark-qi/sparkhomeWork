package com.qi.spark_test.rdd_test

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

object SparkPairRddApiTest {

  val conf = new SparkConf().setAppName("kvrddapiTest").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  def mapValuesTest(): Unit = {
    val list = List((1, "abc"), (2, "def"), (3, "aaa"), (4, "xyz"), (1, "yui"), (2, "eee"))
    val kvRdd = sc.parallelize(list)
    val result = kvRdd.mapValues(x => x(0))

    result.foreach(println)
  }

  def mapTest(): Unit = {
    val list = List((1, "abc"), (2, "def"), (3, "aaa"), (4, "xyz"), (1, "yui"), (2, "eee"))
    val kvRdd = sc.parallelize(list)
    val result = kvRdd.map(x => (x._1, x._2(0)))
    result.foreach(println)
  }

  def flatMapValueTest(): Unit = {

    val list = List(("1.txt", "df df dfb wefr rgfer fgre grt ghrt hrt hj4rytg er"),
      ("2.txt", "dfb fdgav yj oi,lrthb tyjnuy uim ky y6uj  jtrhh re"),
      ("3.txt", "ijhn fnj erh sdnbv ty';jl yukl kyu6k rthjih  th"))
    val kvRdd = sc.parallelize(list)
    val result = kvRdd.flatMapValues(x => x.split("\\s+")).map(x => (x._1, 1)).reduceByKey(_ + _)
    result.foreach(println)

  }

  def reduceByKeyTest(): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 11, 13, 15))
    val kvrdd = rdd.map(x => ((if (x % 2 == 0) "偶数" else "奇数"), x))

    val result = kvrdd.mapValues(x => 1).reduceByKey(_ + _)
    result.foreach(println)

  }

  def foldByKey(): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 11, 13, 15), 2)
    val kvrdd = rdd.map(x => ((if (x % 2 == 0) "偶数" else "奇数"), x))

    //    val result=kvrdd.foldByKey(0)((x1,x2)=>x1+1)
    val result = kvrdd.mapValues(x=>1).foldByKey(0)((x1, x2) => x1 + x2)
    result.foreach(println)
  }

  def aggregateByKeyTest(): Unit ={
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 11, 13, 15), 2)
    val kvrdd = rdd.map(x => ((if (x % 2 == 0) "偶数" else "奇数"), x))
    val result=kvrdd.aggregateByKey(0)((x1,x2)=>x1+1,(x1,x2)=>x1+x2)
    result.foreach(println)
  }


  def combineBykeyTest(): Unit ={
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 11, 13, 15), 2)
    val kvrdd = rdd.map(x => ((if (x % 2 == 0) "偶数" else "奇数"), x))


    //x1,x2都为value值,并且x=>1是将每个分区开头的value值改为1
    val result=kvrdd.combineByKey(x=>1,(x1:Int,x2:Int)=>x1+1,(x1:Int,x2:Int)=>x1+x2)

    result.foreach(println)
  }

//现shuffle后聚合,效率较低
  def groupByKeyTest(): Unit ={
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 11, 13, 15), 2)
    val kvrdd = rdd.map(x => ((if (x % 2 == 0) "偶数" else "奇数"), x))

    val groupByRdd=kvrdd.groupByKey()
    val countValues=groupByRdd.foreach(x=>println(s"${x._1}-->${x._2.size}"))
  }

  def countByKeyTest(): Unit ={
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 11, 13, 15), 2)
    val kvrdd = rdd.map(x => ((if (x % 2 == 0) "偶数" else "奇数"), x))
    val result=kvrdd.countByKey()

    result.foreach(println)
  }

  def reverseIndex(): Unit ={

    val list = List(("1.txt", "df df dfb wefr rgfer fgre yukl ijhn hrt ijhn er"),
      ("2.txt", "dfb se yj re sdnbv tyjnuy sdnbv se se ky y6uj  jtrhh ijhn"),
      ("3.txt", "ijhn se erh th sdnbv tyjl yukl kyu6k rthjih sd sd th"))

    val rdd=sc.parallelize(list)

    val result= rdd.flatMapValues(x=>x.split("\\s+")).map(x=>((x._2,x._1),1)).reduceByKey(_+_).
      map(x=>(x._1._1,x._1._2+"("+x._2+")")).reduceByKey((x1,x2)=>x1+"->"+x2)

    result.foreach(println)
  }
//.reduceByKey((x1,x2)=>x1+"->"+x2)

  def joinTest(): Unit ={
    val user=sc.parallelize(List((1,"张三"),(2,"李四"),(3,"王五")))
    val userDep=sc.parallelize(List((1,"销售部"),(2,"人事部"),(3,"研发部")))
    val innerJoin=user.join(userDep)
    val leftJoin=user.leftOuterJoin(userDep)
    val leftJoin1=userDep.leftOuterJoin(user)

    innerJoin.foreach(x=>println(s"key:${x._1},前表value:${x._2._1},后表value:${x._2._2}"))
    println("==========================================")
    leftJoin.foreach(x=>println(s"key:${x._1},前表value:${x._2._1},后表value:${x._2._2}"))
    println("==========================================")
    leftJoin1.foreach(x=>println(s"key:${x._1},前表value:${x._2._1},后表value:${x._2._2}"))
  }

//cogroup=groupByKey+fullOuterJoin
  def cogroupTest(): Unit ={
    val rdd1=sc.parallelize(List((1,"张三"),(2,"李四"),(3,"王五")))
    val rdd2=sc.parallelize(List((1,("语文","87")),(1,("数学","33")),(2,("英语","44")),(5,("汉语","87"))))

    val result=rdd1.cogroup(rdd2)

    result.foreach(x=>println(s"id:${x._1},姓名:${x._2._1},成绩:${x._2._2}"))
  }


  def saveAsSequenceFile(): Unit ={
    val rdd1=sc.parallelize(List((1,"张三"),(2,"李四"),(3,"王五")))
    rdd1.map(x=>(new IntWritable(x._1),new Text(x._2)))
      .saveAsNewAPIHadoopFile("file:///e:/userSparkoutput",classOf[IntWritable],classOf[Text],classOf[SequenceFileOutputFormat[IntWritable,Text]])

    val rdd2=sc.newAPIHadoopFile("file:///e:/userSparkoutput",classOf[SequenceFileInputFormat[IntWritable,Text]],classOf[IntWritable],classOf[Text])

    rdd2.foreach(println)
  }


  def main(args: Array[String]): Unit = {

    //    mapValuesTest()
    //    mapTest()
    //    flatMapValueTest()

    //    reduceByKeyTest()
//    foldByKey()

//    aggregateByKeyTest()

//    combineBykeyTest()

//    reverseIndex()

//    joinTest()

//    groupByKeyTest()

//    countByKeyTest()
//    cogroupTest()

    saveAsSequenceFile()
  }
}
