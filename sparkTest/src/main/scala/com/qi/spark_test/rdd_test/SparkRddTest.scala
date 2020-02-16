package com.qi.spark_test.rdd_test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkRddTest {

  val conf =new SparkConf().setAppName("apitest").setMaster("local[*]")
  val sc=SparkContext.getOrCreate(conf)

  //算子的输入是一行记录,输出也是一行记录/一个元素
  def mapTest(): Unit ={

    val list=List(1,2,3,4,5,6,7,8)
    val rdd=sc.parallelize(list,3)
    val result=rdd.map(x=>{
      println("加载第三方资源")
      x*x
    })

    result.foreach(println)
  }
  //算子的输入是一个的分区的数据组成的集合,输出也会是一个集合会被变为一个分区
  def mapPartitionTest(): Unit ={

      val rdd=sc.range(1,9,numSlices = 3)
    val result=rdd.mapPartitions(x=>{
      println("=========加载第三方资源===============")
      for (i<-x) yield i*i
    })

  }

  //mapPartitions实现flatmap功能
  def flatMapTest()={
    val lines=List("hadoop mapreduce world hdfs","spark sparksql sparkstring")
    val rdd=sc.makeRDD(lines,1)
    val result=rdd.mapPartitions(x=>{
      val lb=ListBuffer[String]()
      for(line<-x){
        line.split("\\s+").foreach(w=>lb+=w)
      }
      lb.toIterator
    })


  }

  def keyByTest(): Unit ={
    val list=List("apple","banana","pear","tomato")
    var rdd=sc.parallelize(list)
    val result=rdd.keyBy(x=>x.length)

    result.foreach(println)
  }

  def aggregateTest(): Unit ={
    val list=List(1,5,2,6,3,8,4,7,2)
    val rdd=sc.parallelize(list,3)
    val result=rdd.aggregate("")((x:String,y:Int)=>{if(x=="") y.toString else x.toString+"->"+y.toString},
      (x1:String,x2:String)=>x1+"->"+x2)
    println(result)
  }

  def groupByTest(): Unit ={
    val list=List("apple","banana","pear","book","car","able","cup","computer")
    val rdd=sc.parallelize(list,2)
    val result=rdd.groupBy(x=>x(0))
    result.foreach(println)
  }

  def topNtest(): Unit ={
    val list=List(34,62,74,21,41,35,11,42,23,43)
    val rdd=sc.parallelize(list,2)
    val ltopN=rdd.takeOrdered(3)
    val topN=rdd.top(3)
    println(s"从小到大top3:${ltopN.mkString(",")}")
    println(s"从大到小top3:${topN.mkString(",")}")
  }

  def collectionOper(): Unit ={
    val list1=List(1,3,4,5,7,9)
    val list2=List(2,3,4,6,7,8)
    val rdd1=sc.parallelize(list1)
    var rdd2=sc.parallelize(list2)

    val unionRdd=rdd1.union(rdd2)
    val intersectionRdd=rdd1.intersection(rdd2)
    val subtractRdd=rdd1.subtract(rdd2)
    val cartesianRdd=rdd1.cartesian(rdd2)
    println("==================并集=================")
    unionRdd.collect().foreach(println)

    println("===================减集===============")

    subtractRdd.collect().foreach(println)

    println("=================交集================")

    intersectionRdd.collect().foreach(println)

    println("===============笛卡尔集===============")

    cartesianRdd.collect().foreach(println)

  }

  def zipTest(): Unit ={
    val rdd1=sc.parallelize(List(1,3,5,7,9))
    val rdd2=sc.parallelize(List(2,4,6,8,10))
    val zipResult=rdd1.zip(rdd2)

    zipResult.foreach(println)

  }

  def main(args: Array[String]): Unit = {
//    mapTest()
//    mapPartitionTest()
//    keyByTest()
//    aggregateTest()

//    groupByTest()
//    topNtest()
//    collectionOper()
    zipTest()
  }


}
