package com.qi.spark_test.rdd_test_teacher

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by ThinkPad on 2018/4/3.
  */
object SparkRddApiTest {
  val conf = new SparkConf().setAppName("apitest").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  //算子的输入是一行记录/一个元素,输出也是一行记录/一个元素
  def mapTest() = {
    val list = List(1,2,3,4,5,6,7,8)
    val rdd = sc.parallelize(list,3)
    val result = rdd.map(x=>{
      println("加载第三方的资源")
      x*x
    })
    result.foreach(println)
  }
  //算子的输入是一个分区的数据组成的集合,输出也是一个集合会被变成一个分区
  def mapPartitionTest() = {
    val rdd = sc.range(1, 9, numSlices = 3)
    val result = rdd.mapPartitions(x=>{
      println("加载第三方的资源")
      for(i <- x) yield i*i
    })
    result.foreachPartition(x=>{
      x.foreach(println)
    })
  }
  //使用mapPartition实现flatMap功能
  def flatMapTest() = {
    val lines = List("hadoop mapreduce world hdfs","spark sparksql sparkstreaming")
    val rdd = sc.makeRDD(lines, 1)
    val resut = rdd.mapPartitions(x=>{
      val lb = ListBuffer[String]()
      for(line <- x){
        line.split("\\s+").foreach(w=>lb += w) //每一个单词添加到lb中
      }
      lb.toIterator //把listbuffer转换成算子要求的输出类型
    })
    resut.foreach(println)
  }
  //使用mappartition实现filter功能

  //转换成kvrdd
  def keyByTest() = {
    val list = List("apple","banana","pear","tomato")
    val rdd = sc.parallelize(list)
    val result = rdd.keyBy(x=>x.length)

    result.foreach(println)
  }
  //用map实现keyby功能

  //使用aggregate来把rdd的元素，用-->连接起来生成一个字符串
  def aggregateTest() = {
    val list = List(1,5,2,6,3,8,4,7,2)
    val rdd = sc.parallelize(list, 3)
    //1-->5-->2-->6-->...

  }
  //计算rdd中的偶数的数量，使用countByValue

  //group by按照单词首字母进行分组
  def groupByTest() = {
    val list = List("apple","banana","pear","book","car","able","cup","computor")
    val rdd = sc.parallelize(list,2)
    val result = rdd.groupBy(x => x(0))
    result.foreach(println)
  }
  def collectionOper() = {
    val list1 = List(1,3,4,5,7,9)
    val list2 = List(2,3,4,6,7,8)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)

    val unionRdd = rdd1.union(rdd2)
    val intersectionRdd = rdd1.intersection(rdd2)
    val subtractRdd = rdd1.subtract(rdd2)
    val cartesianRdd = rdd1.cartesian(rdd2)
    println("----------并集--------------")
    unionRdd.collect().foreach(println)  //collect把executor上rdd的分区数据传递到driver上形成一个集合对象

    println("交集----" + intersectionRdd.collect().mkString(","))

    println("减集----" + subtractRdd.collect().mkString(","))
    println("笛卡尔乘积------------------------")
    cartesianRdd.collect().foreach(println)
  }
  def zipTest() = {
    val rdd1 = sc.parallelize(List(1,3,5,7,9))
    val rdd2 = sc.parallelize(List(2,4,6,8,10))
    val zipResult = rdd1.zip(rdd2)

    zipResult.foreach(println)
  }
  def topNTest() = {
    val list = List(34,62,74,21,41,35,11,42,23,43)
    val rdd = sc.parallelize(list,2)
    val ltopN = rdd.takeOrdered(3)
    val topN = rdd.top(3)
    println(s"从小到大top3：${ltopN.mkString(",")}")
    println(s"从大到小top3：${topN.mkString(",")}")
  }


  def main(args: Array[String]): Unit = {
//    mapTest()
//    mapPartitionTest()
//    flatMapTest()
//    keyByTest()
//    groupByTest()
//    topNTest()
//    collectionOper()
    zipTest()
  }
}
